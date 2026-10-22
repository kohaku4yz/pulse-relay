"""Unit tests for the live step-activity detector (synthetic UTC fixtures only)."""
from datetime import datetime,timedelta,timezone
import unittest

from pulse_relay.extensions.vitals.activity_transitions_core import (
    ActivityConfig,ActivityState,StepWindow,observe_steps
)

NOW=datetime(2026,10,12,8,0,tzinfo=timezone.utc)

def bucket(n, steps, minutes=5, *, complete=True, source="pulshealth",device_filter="all",received=None):
    start=NOW+timedelta(minutes=n*minutes)
    end=start+timedelta(minutes=minutes)
    return StepWindow(start,end,steps,received or end,source,device_filter,complete)

def apply(items,config=ActivityConfig(),initial=ActivityState(),now=None):
    state=initial
    outputs=[]
    for sample in items:
        state, evt=observe_steps(state,sample,now=now or sample.received_at,config=config)
        if evt:outputs.append(evt)
    return state,outputs

class ActivityTransitionCoreTests(unittest.TestCase):
    def test_first_quiet_baseline_then_started_then_settled(self):
        seq=[bucket(0,0),bucket(1,0),bucket(2,50),bucket(3,55),
             bucket(4,50),bucket(5,0),bucket(6,0)]
        state, events=apply(seq)
        self.assertEqual([x.reason for x in events],["movement_started","movement_settled"])
        self.assertEqual([x.from_mode for x in events],["quiet","moving"])
        self.assertEqual(state.mode,"quiet")

    def test_first_two_moving_windows_not_a_change(self):
        state,events=apply([bucket(0,70),bucket(1,68)])
        self.assertEqual(state.mode,"moving")
        self.assertEqual(events,[])

    def test_unknown_middle_intensity_breaks_pending_confirmation(self):
        seq=[bucket(0,0),bucket(1,0),bucket(2,50),bucket(3,15),bucket(4,50),bucket(5,50)]
        state,events=apply(seq)
        self.assertEqual([e.reason for e in events],["movement_started"])

    def test_duplicate_window_never_repeats_event(self):
        seq=[bucket(0,0),bucket(1,0),bucket(2,50),bucket(3,50)]
        st,events=apply(seq)
        self.assertEqual(len(events),1)
        again,dupe=observe_steps(st,seq[-1],now=seq[-1].received_at)
        self.assertEqual(again,st)
        self.assertIsNone(dupe)

    def test_old_backfill_does_not_generate_event_or_change_state(self):
        st,_=apply([bucket(0,0),bucket(1,0)])
        win=bucket(2,80)
        st2,e=observe_steps(st,win,now=win.end+timedelta(hours=3))
        self.assertEqual(st2,st)
        self.assertIsNone(e)

    def test_missing_window_is_not_evidence_of_rest(self):
        st,_=apply([bucket(0,60),bucket(1,60)])
        st2,events=apply([bucket(5,0),bucket(6,0)],initial=st)
        self.assertFalse(events)
        self.assertEqual(st2.mode,"quiet")  # new baseline from an observation gap

    def test_reject_mixed_source_and_iphone_double_count(self):
        with self.assertRaises(ValueError):bucket(0,15,source="hae")
        with self.assertRaises(ValueError):bucket(0,15,device_filter="watch")
        with self.assertRaises(ValueError):bucket(0,float('nan'))

    def test_reject_day_aggregate_for_quick_mode(self):
        big=bucket(0,0,minutes=1440)
        st,event=observe_steps(ActivityState(),big,now=big.end)
        self.assertEqual(st,ActivityState())
        self.assertIsNone(event)

    def test_hourly_mode_is_coarse_but_supported(self):
        st,ev=apply([bucket(0,0,60),bucket(1,600,60),bucket(2,600,60)])
        self.assertEqual(st.mode,"moving")
        self.assertEqual([e.reason for e in ev],["movement_started"])
        self.assertEqual(ev[0].evidence_window_min,60)

    def test_no_hardcoded_quiet_hours_or_personal_baselines(self):
        early=NOW.replace(hour=2)
        steps=[StepWindow(early+timedelta(minutes=5*i),
                    early+timedelta(minutes=5*(i+1)),v,
                    early+timedelta(minutes=5*(i+1))) for i,v in enumerate([0,0,45,46])]
        _,events=apply(steps)
        self.assertEqual(len(events),1)

    # Added on the move (not in the original 17): no ported test pinned the
    # two-window confirmation; a confirm_windows=1 mutation survived without it.
    def test_single_short_moving_window_is_not_confirmed(self):
        state,events=apply([bucket(0,0),bucket(1,0),bucket(2,50)])
        self.assertEqual(events,[])
        self.assertEqual(state.mode,"quiet")
        self.assertEqual((state.pending,state.pending_count),("moving",1))
        state,events=apply([bucket(3,0)],initial=state)
        self.assertEqual(events,[])
        self.assertEqual((state.mode,state.pending,state.pending_count),("quiet","",0))

if __name__=='__main__':unittest.main()
