"""Owner-approved full candidate read without filesystem permission changes."""
import json,sqlite3,tempfile,unittest
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
from pulse_relay.extensions.vitals.status import read_vitals_status
from pulse_relay.extensions.vitals.tick import vitals_tick

T=datetime(2026,10,12,12,tzinfo=timezone.utc)
class VitalsCandidateReadTests(unittest.TestCase):
    def test_exact_candidate_fields_and_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            data=Path(tmp)
            with patch('pulse_relay.extensions.vitals.tick.read_puls_step_buckets',return_value=[]),patch('pulse_relay.extensions.vitals.tick.read_puls_heart_windows',return_value=[]),patch('pulse_relay.extensions.vitals.tick.read_puls_sleep_evidence',return_value=[]):
                vitals_tick(data,data/'vitals',now=T)
            path=data/'vitals'/'state.sqlite3'
            with sqlite3.connect(path) as db:
                for index in range(3):
                    db.execute('INSERT INTO vitals_candidates(event_id,event_name,observed_at,trigger_names,recorded_at) VALUES(?,?,?,?,?)',(f'evt_{index}','life.vitals.changed',f'2026-10-12T12:0{index}:00Z',json.dumps(['heart_rate_rose','movement_started']),f'2026-10-12T12:0{index}:00Z'))
            before=path.stat().st_mtime_ns
            result=read_vitals_status(data,limit=2)
            self.assertEqual(result['total_candidates'],3)
            self.assertEqual(len(result['recent_candidates']),2)
            self.assertEqual(result['recent_candidates'][0]['observed_at'],'2026-10-12T12:02:00Z')
            self.assertEqual(result['recent_candidates'][0]['triggers'],['heart_rate_rose','movement_started'])
            self.assertEqual(result['events_sent'],0)
            self.assertEqual(path.stat().st_mtime_ns,before)
if __name__=='__main__': unittest.main()
