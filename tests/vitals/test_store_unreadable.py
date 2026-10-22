"""An unreadable health store must fail loudly, never look like "no data"."""
from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from pulse_relay.extensions.vitals import tick as vitals
from pulse_relay.extensions.vitals.activity_windows import read_puls_step_buckets
from pulse_relay.extensions.vitals.signal_windows import (
    read_puls_heart_windows, read_puls_sleep_evidence)

T = datetime(2026, 10, 12, 11, tzinfo=timezone.utc)


class UnreadableStoreTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.data = self.root/'data'
        self.data.mkdir()
        # A file that opens but cannot be queried: the same OperationalError
        # path as a WAL DB whose -shm the reader may not create.
        sqlite3.connect(self.data/'health.sqlite3').close()

    def test_readers_raise_instead_of_returning_empty(self):
        for read in (lambda: read_puls_step_buckets(self.data, now=T),
                     lambda: read_puls_heart_windows(self.data, now=T),
                     lambda: read_puls_sleep_evidence(self.data, now=T)):
            with self.assertRaises(sqlite3.OperationalError):
                read()

    def test_missing_store_is_still_empty(self):
        missing = self.root/'nothing-here'
        self.assertEqual(read_puls_step_buckets(missing, now=T), [])
        self.assertEqual(read_puls_heart_windows(missing, now=T), [])
        self.assertEqual(read_puls_sleep_evidence(missing, now=T), [])

    def test_tick_fails_the_run_and_keeps_detector_state(self):
        state = self.root/'state'
        out = io.StringIO()
        argv = ['tick', '--data-dir', str(self.data), '--state-dir', str(state)]
        with patch('sys.argv', argv), redirect_stdout(out):
            self.assertEqual(vitals.main(), 1)
        self.assertEqual(json.loads(out.getvalue()),
                         {'error': 'store_unreadable', 'type': 'OperationalError'})
        with sqlite3.connect(state/'state.sqlite3') as db:
            # Rolled back: no bootstrap marker, no detector state, no tick time.
            self.assertEqual(db.execute('SELECT COUNT(*) FROM metadata').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM detector_state').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
