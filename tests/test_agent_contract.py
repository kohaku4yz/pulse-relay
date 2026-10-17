"""Contract for agent adapters: the fields the compact health client reads.

Both agent adapters (the MCP server in this repo and the health tools in haru-vps-mcp)
copy the same compact client. If one of these shapes changes, update both copies.
"""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from legacy_hae import ingest_hae_payload
from pulse_relay.store import health_day, heartbeat
from test_service import payload

NOW = datetime(2026, 9, 29, 11, 30, tzinfo=timezone.utc)


class AgentContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)
        ingest_hae_payload(self.data_dir, payload(), received_at=datetime(2026, 9, 29, 11, 10, tzinfo=timezone.utc))

    def tearDown(self):
        self.tmp.cleanup()

    def test_day_carries_every_field_the_compact_client_reads(self):
        day = health_day(self.data_dir, now=NOW)
        self.assertLessEqual({"status", "date", "freshness_seconds", "latest", "activity", "sleep", "heart_rate"}, set(day))
        self.assertIsInstance(day["freshness_seconds"], int)

        self.assertTrue(day["latest"])
        for item in day["latest"].values():
            self.assertIsInstance(item["value"], dict)
            self.assertIsInstance(item["units"], str)
            self.assertIsInstance(item["age_seconds"], int)

        self.assertTrue(day["activity"])
        for item in day["activity"].values():
            self.assertIsInstance(item["qty"], (int, float))
            self.assertIsInstance(item["units"], str)

        self.assertIsInstance(day["sleep"]["value"], dict)
        self.assertIsInstance(day["sleep"]["units"], str)

        self.assertLessEqual({"count", "units", "latest", "last_observed_at", "avg", "min", "max"}, set(day["heart_rate"]))

    def test_heartbeat_carries_every_field_the_compact_client_reads(self):
        beat = heartbeat(self.data_dir, now=NOW)
        self.assertEqual(beat["status"], "ok")
        self.assertIsInstance(beat["bpm"], (int, float))
        self.assertIsInstance(beat["observed_at"], str)
        self.assertIsInstance(beat["age_seconds"], int)
        self.assertIsInstance(beat["freshness_seconds"], int)


if __name__ == "__main__":
    unittest.main()
