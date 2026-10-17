"""The core service must run with pulse_relay/extensions/ removed."""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PROBE = textwrap.dedent(
    """
    import http.client, sys, tempfile, threading
    from pathlib import Path
    from pulse_relay.server import ServiceSettings, build_http_server

    with tempfile.TemporaryDirectory() as tmp:
        server = build_http_server(ServiceSettings("127.0.0.1", 0, Path(tmp), "z" * 32, 5242880))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for path in ("/v1/health/heartbeat", "/v1/health/day", "/v1/health/not-installed-a", "/v1/health/not-installed-b"):
                conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
                conn.request("GET", path)
                response = conn.getresponse()
                response.read()
                print(path, response.status)
                conn.close()
        finally:
            server.shutdown()
            server.server_close()
    print("extensions_loaded", any(name.startswith("pulse_relay.extensions") for name in sys.modules))
    """
)


DEMO_ROUTES = """
ROUTES = {"/v1/health/demo": lambda data_dir, query: (200, {"demo": query})}
"""


def copy_core(tmp: str, extensions: dict[str, str]) -> None:
    """Copy the package without its installed extensions, then add demo ones."""
    package = Path(tmp) / "pulse_relay"
    shutil.copytree(ROOT / "pulse_relay", package, ignore=shutil.ignore_patterns("extensions", "__pycache__"))
    if extensions:
        (package / "extensions").mkdir()
        (package / "extensions/__init__.py").write_text("")
        for name, routes in extensions.items():
            (package / "extensions" / name).mkdir()
            (package / "extensions" / name / "__init__.py").write_text("")
            if routes is not None:
                (package / "extensions" / name / "routes.py").write_text(routes)


def probe(tmp: str, *paths: str) -> subprocess.CompletedProcess[str]:
    script = PROBE.replace(
        '("/v1/health/heartbeat", "/v1/health/day", "/v1/health/not-installed-a", "/v1/health/not-installed-b")',
        repr(("/v1/health/heartbeat", "/v1/health/day", "/v1/health/not-installed-a", "/v1/health/not-installed-b", *paths)),
    )
    return subprocess.run(
        [sys.executable, "-I", "-c", f"import sys; sys.path.insert(0, {tmp!r})\n" + script],
        capture_output=True,
        text=True,
        timeout=30,
        cwd=tmp,
    )


class CoreWithoutExtensionsTests(unittest.TestCase):
    def test_core_routes_work_and_extension_routes_are_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            copy_core(tmp, {})
            result = probe(tmp)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = set(result.stdout.splitlines())
        self.assertIn("/v1/health/heartbeat 200", lines)
        self.assertIn("/v1/health/day 200", lines)
        self.assertIn("/v1/health/not-installed-a 404", lines)
        self.assertIn("/v1/health/not-installed-b 404", lines)
        self.assertIn("extensions_loaded False", lines)

    def test_any_installed_extension_is_discovered(self):
        with tempfile.TemporaryDirectory() as tmp:
            copy_core(tmp, {"demo": DEMO_ROUTES, "no_routes": None})
            result = probe(tmp, "/v1/health/demo")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("/v1/health/demo 200", result.stdout.splitlines())

    def test_installed_but_broken_extension_fails_loudly(self):
        with tempfile.TemporaryDirectory() as tmp:
            copy_core(tmp, {"demo": "import missing_dependency\n"})
            result = probe(tmp)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing_dependency", result.stderr)

    def test_two_extensions_claiming_one_path_fail_loudly(self):
        with tempfile.TemporaryDirectory() as tmp:
            copy_core(tmp, {"demo": DEMO_ROUTES, "demo_twin": DEMO_ROUTES})
            result = probe(tmp)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("/v1/health/demo", result.stderr)

    def test_core_modules_do_not_name_extension_code(self):
        for path in (ROOT / "pulse_relay").glob("*.py"):
            text = path.read_text()


if __name__ == "__main__":
    unittest.main()
