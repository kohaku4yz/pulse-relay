"""Durable Vitals contract, synthetic evidence; no health DB or HTTP."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime,timedelta,timezone
from pathlib import Path
from unittest.mock import patch

from pulse_relay.extensions.vitals.tick import vitals_tick
from pulse_relay.extensions.vitals.signal_windows import PulsHeartBucket,PulsSleepEvidence
from pulse_relay.extensions.vitals.activity_windows import PulsStepBucket

T=datetime(2026,10,12,11,tzinfo=timezone.utc)


def h(i,value,*,late_minutes=1):
    begin=T-timedelta(minutes=60)+timedelta(minutes=i*15)
    end=begin+timedelta(minutes=15)
    return PulsHeartBucket(begin,end,float(value),5,end+timedelta(minutes=late_minutes))


def s(i,value):
    begin=T-timedelta(minutes=20)+timedelta(minutes=i*5)
    end=begin+timedelta(minutes=5)
    return PulsStepBucket(begin,end,float(value),end)


class PersistentTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.state=self.root/'private-state'
        self.data=self.root/'fake-health-dir'

    def tick(self,*,when=T,steps=(),heart=(),sleep=()):
        with patch('pulse_relay.extensions.vitals.tick.read_puls_step_buckets',return_value=list(steps)),\
             patch('pulse_relay.extensions.vitals.tick.read_puls_heart_windows',return_value=list(heart)),\
             patch('pulse_relay.extensions.vitals.tick.read_puls_sleep_evidence',return_value=list(sleep)):
            return vitals_tick(self.data,self.state,now=when)

    def table(self):
        with sqlite3.connect(self.state/'state.sqlite3') as db:
            return db.execute('SELECT event_name,trigger_names FROM vitals_candidates ORDER BY observed_at').fetchall()

    def test_bootstrap_no_alert_then_new_heart_then_replay_once(self):
        first=self.tick(heart=[h(0,70),h(1,70)])
        self.assertTrue(first['bootstrap'])
        self.assertEqual(first['saved_vitals_candidates'],0)
        second=self.tick(when=T+timedelta(minutes=1),heart=[h(0,70),h(1,70),h(2,95),h(3,96)])
        self.assertFalse(second['bootstrap'])
        self.assertEqual(second['new_vitals_candidates'],1)
        self.assertEqual(self.table(),[('life.vitals.changed','["heart_rate_rose"]')])
        replay=self.tick(when=T+timedelta(minutes=2),heart=[h(0,70),h(1,70),h(2,95),h(3,96)])
        self.assertEqual(replay['new_vitals_candidates'],0)
        self.assertEqual(replay['saved_vitals_candidates'],1)
        self.assertEqual(os.stat(self.state).st_mode & 0o777,0o700)
        self.assertEqual(os.stat(self.state/'state.sqlite3').st_mode & 0o777,0o600)

    def test_first_scan_can_include_change_but_suppresses_retroactive_candidate(self):
        result=self.tick(when=T+timedelta(minutes=1),heart=[h(0,70),h(1,70),h(2,95),h(3,96)])
        self.assertTrue(result['bootstrap'])
        self.assertEqual(result['new_vitals_candidates'],0)
        self.assertEqual(self.table(),[])
        result=self.tick(when=T+timedelta(minutes=3),heart=[h(0,70),h(1,70),h(2,95),h(3,96)])
        self.assertEqual(result['saved_vitals_candidates'],0)

    def test_activity_independent_of_heart_and_dedup(self):
        self.tick(when=T,steps=[s(0,0),s(1,0)])
        r=self.tick(when=T+timedelta(minutes=1),steps=[s(0,0),s(1,0),s(2,45),s(3,55)])
        self.assertEqual(r['new_vitals_candidates'],1)
        self.assertEqual(self.table()[0][1],'["movement_started"]')

    def test_two_sensors_in_one_poll_share_one_candidate(self):
        self.tick(heart=[h(0,70),h(1,70)],steps=[s(0,0),s(1,0)])
        report=self.tick(when=T+timedelta(minutes=1),
                         heart=[h(0,70),h(1,70),h(2,95),h(3,96)],
                         steps=[s(0,0),s(1,0),s(2,45),s(3,55)])
        self.assertEqual(report['new_vitals_candidates'],1)
        names=json.loads(self.table()[0][1])
        self.assertEqual(names,['heart_rate_rose','movement_started'])
        self.assertEqual(len(self.table()),1)

    def test_wake_is_candidate_only_after_new_explicit_evidence(self):
        asleep=PulsSleepEvidence(T-timedelta(minutes=90),T-timedelta(minutes=89),'asleep',160)
        woke=PulsSleepEvidence(T-timedelta(minutes=10),T-timedelta(minutes=9),'awake',40)
        self.tick(sleep=[asleep])
        r=self.tick(sleep=[asleep,woke])
        self.assertEqual(r['new_vitals_candidates'],1)
        self.assertEqual(self.table()[0][1],'["woke_up"]')

    def test_bootstrap_replayed_old_data_no_fake_wake_or_hourly_alert(self):
        r=self.tick(when=T+timedelta(hours=3),heart=[h(0,70),h(1,70),h(2,100),h(3,101)])
        self.assertEqual(r['new_vitals_candidates'],0)
        self.assertEqual(r['saved_vitals_candidates'],0)

    def test_db_symlink_is_rejected(self):
        self.state.mkdir(mode=0o700)
        real=self.root/'different.sqlite3'
        real.touch(mode=0o600)
        (self.state/'state.sqlite3').symlink_to(real)
        with self.assertRaises(ValueError):self.tick()
        self.assertEqual(real.stat().st_size,0)

    def test_other_schema_version_refuses_without_resetting_history(self):
        self.tick()
        with sqlite3.connect(self.state/'state.sqlite3') as db:
            db.execute("UPDATE metadata SET value='unknown-new-version' WHERE name='schema'")
        with self.assertRaisesRegex(ValueError,'version mismatch'):self.tick()

if __name__=='__main__':unittest.main()
