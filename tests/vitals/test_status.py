"""Puls-only read-only Vitals state tests."""
from __future__ import annotations
import tempfile, unittest
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
from pulse_relay.extensions.vitals.status import read_vitals_status
from pulse_relay.extensions.vitals.tick import vitals_tick
T=datetime(2026,10,12,11,tzinfo=timezone.utc)
class VitalsStatusTests(unittest.TestCase):
    def test_initialization_and_reader(self):
        with tempfile.TemporaryDirectory() as root:
            data=Path(root)
            self.assertEqual(read_vitals_status(data)['status'],'unavailable')
            with patch('pulse_relay.extensions.vitals.tick.read_puls_step_buckets',return_value=[]),patch('pulse_relay.extensions.vitals.tick.read_puls_heart_windows',return_value=[]),patch('pulse_relay.extensions.vitals.tick.read_puls_sleep_evidence',return_value=[]):
                vitals_tick(data,data/'vitals',now=T)
            value=read_vitals_status(data)
            self.assertEqual(value['status'],'ok')
            self.assertEqual(value['total_candidates'],0)
            self.assertEqual(value['last_run_at'],'2026-10-12T11:00:00Z')
            self.assertEqual(value['events_sent'],0)
    def test_invalid_limit(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(ValueError):read_vitals_status(Path(root),limit=101)
