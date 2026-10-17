"""deploy/pairing.sh prints a PulsHealth pairing link the app's parser accepts."""
from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy" / "pairing.sh"
TOKEN = "a1" * 32


class PairingScriptTests(unittest.TestCase):
    def run_script(self, *args: str, token: str | None = TOKEN, fake_qr: bool = False) -> subprocess.CompletedProcess[str]:
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        env_file = tmp / "service.env"
        if token is not None:
            env_file.write_text(f"# comment\nPULSE_RELAY_INGEST_TOKEN={token}\n")
        bin_dir = tmp / "bin"
        bin_dir.mkdir()
        self.argv_log = tmp / "argv.log"
        if fake_qr:
            qr = bin_dir / "qrencode"
            qr.write_text(f'#!/bin/sh\necho "$*" >> {self.argv_log}\necho "FAKE-QR $* $(cat)"\n')
            qr.chmod(0o755)
        # Every external tool is wrapped so the test can see each child's argv.
        for tool in ("python3", "sed", "head", "cat"):
            found = subprocess.run(["sh", "-c", f"command -v {tool}"], capture_output=True, text=True).stdout.strip()
            shim = bin_dir / tool
            shim.write_text(f'#!/bin/sh\necho "$*" >> {self.argv_log}\nexec {found} "$@"\n')
            shim.chmod(0o755)
        env = {"PATH": str(bin_dir), "PULSE_RELAY_ENV_FILE": str(env_file)}
        return subprocess.run(["/bin/bash", str(SCRIPT), *args], capture_output=True, text=True, env=env, timeout=10)

    def child_argv(self) -> str:
        return self.argv_log.read_text() if self.argv_log.exists() else ""

    def link(self, stdout: str) -> str:
        lines = [line for line in stdout.splitlines() if line.startswith("puls://pair?")]
        self.assertEqual(len(lines), 1, stdout)
        return lines[0]

    def test_link_matches_pulshealth_pairing_payload(self):
        result = self.run_script("https://health.example.com")
        self.assertEqual(result.returncode, 0, result.stderr)
        parts = urlsplit(self.link(result.stdout))
        self.assertEqual((parts.scheme, parts.netloc), ("puls", "pair"))
        query = parse_qs(parts.query, strict_parsing=True)
        self.assertEqual(query["url"], ["https://health.example.com/api/health-sync"])
        self.assertEqual(query["token"], [TOKEN])
        self.assertEqual(str(uuid.UUID(query["user"][0])), query["user"][0])
        self.assertIn("qrencode", result.stdout)  # install hint when no QR tool

    def test_base_path_and_trailing_slash_are_normalized(self):
        for given in ("https://health.example.com/", "https://health.example.com/api/health-sync/"):
            result = self.run_script(given)
            query = parse_qs(urlsplit(self.link(result.stdout)).query)
            self.assertEqual(query["url"], ["https://health.example.com/api/health-sync"], given)

    def test_reserved_characters_are_percent_encoded(self):
        result = self.run_script("https://health.example.com:8443", token="tok&en=x+y/z")
        link = self.link(result.stdout)
        self.assertIn("token=tok%26en%3Dx%2By%2Fz", link)
        self.assertIn("url=https%3A%2F%2Fhealth.example.com%3A8443%2Fapi%2Fhealth-sync", link)

    def test_prints_qr_when_qrencode_exists(self):
        result = self.run_script("https://health.example.com", fake_qr=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("FAKE-QR -t ANSIUTF8 puls://pair?", result.stdout)

    def test_token_never_appears_in_child_process_arguments(self):
        for fake_qr in (False, True):
            result = self.run_script("https://health.example.com", token="SECRET-" + TOKEN, fake_qr=fake_qr)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("SECRET-", self.link(result.stdout))  # still printed for the operator
            argv = self.child_argv()
            self.assertIn("ANSIUTF8", argv) if fake_qr else self.assertTrue(argv)
            self.assertNotIn("SECRET-", argv)
            self.assertNotIn("url=https%3A", argv)  # no finished link either

    def test_rejects_plain_http_and_bad_input(self):
        for args in ((), ("http://health.example.com",), ("health.example.com",), ("https://health.example.com?x=1",)):
            result = self.run_script(*args)
            self.assertNotEqual(result.returncode, 0, args)
            self.assertNotIn("puls://pair?", result.stdout)

    def test_missing_token_fails_without_printing_a_link(self):
        result = self.run_script("https://health.example.com", token=None)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("puls://pair?", result.stdout)


if __name__ == "__main__":
    unittest.main()
