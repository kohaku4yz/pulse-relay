"""Unit tests for the live heart-rate and sleep detectors (synthetic UTC fixtures only)."""
from datetime import datetime,timedelta,timezone
import unittest

from pulse_relay.extensions.vitals.multisignal_core import (
    HeartState,HeartWindow,observe_heart,SleepState,SleepObservation,observe_sleep,
)
BASE=datetime(2026,10,12,7,tzinfo=timezone.utc)

def hr(n,value,**kwargs):
    start=BASE+timedelta(minutes=n*5)
    end=start+timedelta(minutes=5)
    return HeartWindow(start,end,value,5,end,**kwargs)

def sleep(n,kind,minutes=40,**kwargs):
    observed=BASE+timedelta(minutes=n*40)
    return SleepObservation(observed,observed,kind,minutes,**kwargs)

class HeartCoreTests(unittest.TestCase):
    def test_heart_initial_stable_change_and_return(self):
        state=HeartState()
        events=[]
        for i,rate in enumerate((72,73,88,91,90,73,70)):
            row=hr(i,rate)
            state,event=observe_heart(state,row,now=row.received_at)
            if event:events.append(event.signal)
        self.assertEqual(events,["heart_rate_rose","heart_rate_fell"])

    def test_heart_one_spike_and_duplicate_no_event(self):
        state=HeartState()
        for i,value in enumerate((70,100,72)):
            with self.subTest(window=i,bpm=value):
                state,event=observe_heart(state,hr(i,value),now=hr(i,value).end)
                self.assertIsNone(event)
        a,event=observe_heart(state,hr(2,70),now=hr(2,70).end)
        self.assertEqual(a,state)
        self.assertIsNone(event)

    def test_heart_backfill_and_missing_gap_do_not_emit(self):
        state,_=observe_heart(HeartState(),hr(0,70),now=hr(0,70).end)
        w=hr(1,120)
        self.assertEqual(observe_heart(state,w,now=w.end+timedelta(hours=2)),(state,None))
        state,event=observe_heart(state,hr(10,125),now=hr(10,125).end)
        self.assertIsNone(event)
        self.assertEqual(state.anchor_bpm,125)

    def test_heart_no_raw_hae(self):
        with self.assertRaises(ValueError):hr(0,70,source="hae")
        with self.assertRaises(ValueError):hr(0,float("nan"))

class SleepCoreTests(unittest.TestCase):
    def test_sleep_wake_requires_full_session_evidence(self):
        state=SleepState()
        state,ev=observe_sleep(state,sleep(0,"awake"),now=sleep(0,"awake").received_at)
        self.assertIsNone(ev)
        state,ev=observe_sleep(state,sleep(1,"asleep",25),now=sleep(1,"asleep").received_at)
        self.assertEqual(ev.signal,"sleep_started")
        transient=sleep(2,"awake",5)
        next_state,ev=observe_sleep(state,transient,now=transient.received_at)
        self.assertEqual(next_state,state)
        self.assertIsNone(ev)
        complete=sleep(3,"awake",35)
        state,ev=observe_sleep(state,complete,now=complete.received_at)
        self.assertEqual(state.mode,"awake")
        self.assertEqual(ev.signal,"woke_up")

    def test_sleep_backfill_and_incomplete_ignored(self):
        state=SleepState("asleep",BASE)
        item=sleep(3,"awake",40,complete=False)
        self.assertEqual(observe_sleep(state,item,now=item.received_at),(state,None))
        item=sleep(3,"awake",40)
        self.assertEqual(observe_sleep(state,item,now=item.received_at+timedelta(hours=3)),(state,None))

    def test_sleep_early_ambiguous_awakening_rejected(self):
        state=SleepState("asleep",BASE)
        item=sleep(2,"awake",35,main_session_evidence=False)
        self.assertEqual(observe_sleep(state,item,now=item.received_at),(state,None))

if __name__=='__main__':unittest.main()
