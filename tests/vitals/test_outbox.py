"""Synthetic Puls-only candidates and bounded owner handoff."""
from __future__ import annotations
import json,sqlite3,tempfile,unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
from pulse_relay.extensions.vitals.tick import vitals_tick
from pulse_relay.extensions.vitals.context_snapshot import ContextValue
from pulse_relay.extensions.vitals.outbox import read_vitals_outbox

NOW=datetime(2026,10,12,11,tzinfo=timezone.utc)

class OutboxTests(unittest.TestCase):
 def test_two_changes_one_event_and_provenance(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)
   with patch('pulse_relay.extensions.vitals.tick.read_puls_step_buckets',return_value=[]),patch('pulse_relay.extensions.vitals.tick.read_puls_heart_windows',return_value=[]),patch('pulse_relay.extensions.vitals.tick.read_puls_sleep_evidence',return_value=[]):
    vitals_tick(root,root/'vitals',now=NOW)
   with sqlite3.connect(root/'vitals'/'state.sqlite3') as db:
    db.execute('INSERT INTO vitals_candidates VALUES (?,?,?,?,?)',(
      'vit_'+'a'*48,'life.vitals.changed','2026-10-12T10:50:00Z',
      json.dumps(['heart_rate_rose','movement_started']),'2026-10-12T10:59:00Z'))
   with patch('pulse_relay.extensions.vitals.outbox.read_puls_context',return_value={
       'heart_rate_bpm':ContextValue(95,NOW-timedelta(minutes=8))}):
    response=read_vitals_outbox(root,now=NOW)
   self.assertEqual(response['status'],'ok')
   self.assertEqual(len(response['candidates']),1)
   self.assertEqual(response['candidates'][0]['event_id'],'vit_'+'a'*48)
   self.assertEqual(response['candidates'][0]['triggers'],['heart_rate_rose','movement_started'])
   self.assertEqual(response['snapshot']['heart_rate_bpm']['source'],'pulshealth')
   self.assertNotIn('heart_rate_anchor',str(response))
   self.assertNotIn('sample_uuid',str(response))
   self.assertNotIn('events_sent',str(response))

 def test_missing_state_and_invalid_limit(self):
  with tempfile.TemporaryDirectory() as d:
   resp=read_vitals_outbox(Path(d),now=NOW)
   self.assertEqual(resp['status'],'unavailable')
   self.assertEqual(resp['snapshot'],{})
   for x in (0,101,True,'3'):
    with self.assertRaises(ValueError):read_vitals_outbox(Path(d),now=NOW,limit=x)
if __name__=='__main__':unittest.main()

class EmptyOutboxFastPathTests(unittest.TestCase):
 def test_no_candidates_never_scans_health_context(self):
  with tempfile.TemporaryDirectory() as folder:
   data=Path(folder)
   with patch('pulse_relay.extensions.vitals.tick.read_puls_step_buckets',return_value=[]), \
        patch('pulse_relay.extensions.vitals.tick.read_puls_heart_windows',return_value=[]), \
        patch('pulse_relay.extensions.vitals.tick.read_puls_sleep_evidence',return_value=[]):
    vitals_tick(data,data/'vitals',now=NOW)
   with patch('pulse_relay.extensions.vitals.outbox.read_puls_context',side_effect=AssertionError('empty outbox scanned private health history')):
    result=read_vitals_outbox(data,now=NOW)
   self.assertEqual(result['status'],'ok')
   self.assertEqual(result['snapshot'],{})
   self.assertEqual(result['candidates'],[])

class OldCandidateFastPathTests(unittest.TestCase):
 def test_stale_candidates_do_not_hold_expensive_health_snapshot_scans(self):
  with tempfile.TemporaryDirectory() as folder:
   data=Path(folder)
   with patch('pulse_relay.extensions.vitals.tick.read_puls_step_buckets',return_value=[]), \
        patch('pulse_relay.extensions.vitals.tick.read_puls_heart_windows',return_value=[]), \
        patch('pulse_relay.extensions.vitals.tick.read_puls_sleep_evidence',return_value=[]):
    vitals_tick(data,data/'vitals',now=NOW)
   with sqlite3.connect(data/'vitals'/'state.sqlite3') as db:
    db.execute('INSERT INTO vitals_candidates VALUES (?,?,?,?,?)', (
     'vit_'+'c'*48,'life.vitals.changed','2026-10-12T08:00:00Z',
     json.dumps(['woke_up']),'2026-10-12T08:01:00Z'))
   with patch('pulse_relay.extensions.vitals.outbox.read_puls_context',side_effect=AssertionError('stale candidate scanned health')):
    result=read_vitals_outbox(data,now=NOW)
   self.assertEqual(result['status'],'ok')
   self.assertEqual(result['candidates'],[])
   self.assertEqual(result['snapshot'],{})
