from __future__ import annotations

import asyncio
import json
import tempfile
import sys
import threading
import unittest
from pathlib import Path

from pulse_relay.health_client import HealthClientSettings, health_detail, heartbeat
from pulse_relay.mcp_server import McpSettings, build_server
from pulse_relay.server import ServiceSettings, build_http_server
# Legacy HAE rows are seeded with the test-only writer that lives with the core tests.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from legacy_hae import ingest_hae_payload  # noqa: E402

# The compact output contract shared with haru-vps-mcp's health tools.
DETAIL_KEYS = {"status", "date", "freshness_seconds", "latest", "activity", "sleep", "heart_rate"}
HEARTBEAT_KEYS = {"status", "bpm", "observed_at", "age_seconds", "freshness_seconds"}


def payload():
    return {
        "data": {
            "metrics": [
                {
                    "name": "heart_rate",
                    "units": "count/min",
                    "data": [
                        {
                            "date": "2026-09-29 11:00:00 +0000",
                            "source": "Apple Watch",
                            "Min": 64,
                            "Avg": 79,
                            "Max": 101,
                        }
                    ],
                },
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {
                            "date": "2026-09-29 11:00:00 +0000",
                            "source": "Apple Watch",
                            "qty": 200,
                        }
                    ],
                },
            ]
        }
    }


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_thin_client_reads_day_and_heartbeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            ingest_hae_payload(data_dir, payload())
            settings = ServiceSettings(
                host="127.0.0.1",
                port=0,
                data_dir=data_dir,
                token="a" * 64,
                max_body_bytes=1024 * 1024,
            )
            server = build_http_server(settings)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                client = HealthClientSettings(service_url=f"http://{host}:{port}")
                day = await health_detail(client)
                self.assertEqual(day["status"], "ok")
                self.assertEqual(day["date"], "2026-09-29")
                self.assertEqual(day["activity"]["step_count"], {"qty": 200, "units": "count"})
                self.assertEqual(set(day), DETAIL_KEYS)
                heart = day["latest"]["heart_rate"]
                self.assertEqual(heart["avg"], 79)
                self.assertNotIn("source", heart)
                self.assertNotIn("observed_at", heart)
                self.assertIn("age_seconds", heart)

                explicit = await health_detail(client, "2026-09-29")
                self.assertEqual(explicit["date"], "2026-09-29")

                beat = await heartbeat(client)
                self.assertEqual(beat["status"], "ok")
                self.assertEqual(beat["bpm"], 79)
                self.assertEqual(set(beat), HEARTBEAT_KEYS)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


class SchemaTests(unittest.TestCase):
    def test_exact_tool_surface_and_nullable_output_schema(self):
        server = build_server(
            McpSettings(
                host="127.0.0.1",
                port=8879,
                path="/mcp",
                health_service_url="http://127.0.0.1:8768",
            )
        )
        tools = {tool.name: tool for tool in server._tool_manager.list_tools()}
        self.assertEqual(sorted(tools), ["health_detail", "heartbeat"])

        detail = tools["health_detail"]
        date_input = detail.parameters["properties"]["date"]
        self.assertEqual(date_input.get("default"), None)
        self.assertIn({"type": "null"}, date_input.get("anyOf", []))

        detail_schema = detail.fn_metadata.output_schema
        self.assertIsInstance(detail_schema, dict)
        detail_props = detail_schema["properties"]
        self.assertEqual(set(detail_props), DETAIL_KEYS)
        for field in ("date", "freshness_seconds", "sleep", "heart_rate"):
            self.assertIn({"type": "null"}, detail_props[field]["anyOf"])

        heartbeat_schema = tools["heartbeat"].fn_metadata.output_schema
        heartbeat_props = heartbeat_schema["properties"]
        self.assertEqual(set(heartbeat_props), HEARTBEAT_KEYS)
        for field in ("bpm", "observed_at", "age_seconds", "freshness_seconds"):
            self.assertIn({"type": "null"}, heartbeat_props[field]["anyOf"])


if __name__ == "__main__":
    unittest.main()
