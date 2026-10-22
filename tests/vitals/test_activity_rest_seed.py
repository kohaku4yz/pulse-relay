"""Sleep-covered step gaps seed quiet, so the first walk after waking counts.

Synthetic UTC fixtures only: hourly all-device buckets as PulsHealth sends them.
"""
from datetime import datetime, timedelta, timezone
import json
import tempfile
import unittest
from pathlib import Path

from pulse_relay.extensions.vitals.activity_transitions_core import (
    ActivityState, RestSpan, StepWindow, observe_steps)
from pulse_relay.extensions.vitals import tick as vitals

# Evening, then no buckets overnight (zero-step hours are never stored).
EVENING = datetime(2026, 10, 11, 21, 0, tzinfo=timezone.utc)
SLEEP = RestSpan(datetime(2026, 10, 11, 22, 30, tzinfo=timezone.utc),
                 datetime(2026, 10, 12, 6, 0, tzinfo=timezone.utc))


def hour(start, steps):
    return StepWindow(start, start + timedelta(hours=1), steps, start + timedelta(hours=1))


def run(windows, *, rest=None, state=ActivityState()):
    events = []
    for w in windows:
        state, evt = observe_steps(state, w, now=w.received_at, rest=rest)
        if evt:
            events.append(evt)
    return state, events


EVENING_QUIET = [hour(EVENING - timedelta(hours=1), 30), hour(EVENING, 20)]   # <=1 step/min
MORNING_WALK = hour(datetime(2026, 10, 12, 6, 0, tzinfo=timezone.utc), 900)  # 15 steps/min


class RestSeedTests(unittest.TestCase):
    def test_without_sleep_first_walk_after_gap_is_silent(self):
        state, events = run(EVENING_QUIET + [MORNING_WALK])
        self.assertEqual(events, [])
        self.assertEqual(state.mode, 'moving')  # silent baseline, as before

    def test_sleep_covered_gap_reports_first_walk(self):
        state, events = run(EVENING_QUIET + [MORNING_WALK], rest=SLEEP)
        self.assertEqual([e.reason for e in events], ['movement_started'])
        self.assertEqual(events[0].from_mode, 'quiet')
        self.assertEqual(state.rest_seed_end, SLEEP.end)
        self.assertIsNone(state.seed_until)

    def test_sleep_that_does_not_cover_the_gap_stays_unknown(self):
        late_start = RestSpan(EVENING + timedelta(hours=3), SLEEP.end)  # 2 h after last bucket
        early_end = RestSpan(SLEEP.start, MORNING_WALK.start - timedelta(hours=2))
        for rest in (late_start, early_end):
            _, events = run(EVENING_QUIET + [MORNING_WALK], rest=rest)
            self.assertEqual(events, [], rest)

    def test_one_seed_per_session(self):
        # A second gap after the same session must not seed again.
        state, events = run(EVENING_QUIET + [MORNING_WALK], rest=SLEEP)
        later = MORNING_WALK.end + timedelta(hours=2)
        _, more = run([hour(later, 0), hour(later + timedelta(hours=2), 900)],
                      rest=SLEEP, state=state)
        self.assertEqual(len(events), 1)
        self.assertEqual(more, [])

    def test_unconfirmed_seed_expires_four_hours_after_waking(self):
        # Ambiguous hours (2-5 steps/min) never confirm quiet; a walk 5 h
        # after waking is no longer "the first movement after waking".
        start = SLEEP.end
        windows = EVENING_QUIET + [hour(start + timedelta(hours=i), 180) for i in range(5)]
        windows.append(hour(start + timedelta(hours=5), 900))
        state, events = run(windows, rest=SLEEP)
        self.assertEqual(events, [])
        self.assertEqual(state.mode, 'moving')

    def test_real_quiet_window_confirms_seed_and_lifts_expiry(self):
        start = SLEEP.end
        windows = EVENING_QUIET + [hour(start, 10)]   # real quiet hour after waking
        windows += [hour(start + timedelta(hours=1 + i), 180) for i in range(5)]
        windows.append(hour(start + timedelta(hours=6), 900))
        _, events = run(windows, rest=SLEEP)
        self.assertEqual([e.reason for e in events], ['movement_started'])


    def test_late_stage_extending_the_same_session_does_not_reseed(self):
        # A late stage moves the session end; still one seed.
        state, events = run(EVENING_QUIET + [MORNING_WALK], rest=SLEEP)
        extended = RestSpan(SLEEP.start, SLEEP.end + timedelta(minutes=10))
        start = MORNING_WALK.end + timedelta(minutes=5)          # 5-minute gap
        short = [StepWindow(start + timedelta(minutes=5 * i), start + timedelta(minutes=5 * (i + 1)),
                            60, start + timedelta(minutes=5 * (i + 1))) for i in range(2)]
        _, more = run(short, rest=extended, state=state)
        self.assertEqual(len(events), 1)
        self.assertEqual(more, [])

    def test_corrected_session_start_is_still_the_same_session(self):
        state, _ = run(EVENING_QUIET + [MORNING_WALK], rest=SLEEP)
        corrected = RestSpan(SLEEP.start - timedelta(minutes=20), SLEEP.end)
        later = MORNING_WALK.end + timedelta(hours=2)
        _, more = run([hour(later, 0), hour(later + timedelta(hours=2), 900)],
                      rest=corrected, state=state)
        self.assertEqual(more, [])

    def test_next_nights_session_seeds_again(self):
        state, _ = run(EVENING_QUIET + [MORNING_WALK], rest=SLEEP)
        day = timedelta(days=1)
        evening2 = [hour(EVENING + day - timedelta(hours=1), 30), hour(EVENING + day, 20)]
        night2 = RestSpan(SLEEP.start + day, SLEEP.end + day)
        _, events = run(evening2 + [hour(MORNING_WALK.start + day, 900)], rest=night2, state=state)
        self.assertEqual([e.reason for e in events], ['movement_started'])

class StateCompatibilityTests(unittest.TestCase):
    def test_ordinary_state_is_written_without_new_fields(self):
        text = vitals._encoded(ActivityState('quiet', '', 0, EVENING))
        self.assertEqual(set(json.loads(text)), {'mode', 'pending', 'pending_count', 'last_end'})

    def test_seeded_state_round_trips(self):
        seeded = ActivityState('quiet', '', 0, None, rest_seed_end=SLEEP.end,
                               seed_until=SLEEP.end + timedelta(hours=4))
        self.assertEqual(vitals._decode(ActivityState, vitals._encoded(seeded)), seeded)

    def test_unknown_keys_are_ignored(self):
        text = json.dumps({'mode': 'quiet', 'pending': '', 'pending_count': 0,
                           'last_end': '2026-10-11T21:00:00Z', 'from_the_future': 1})
        self.assertEqual(vitals._decode(ActivityState, text).mode, 'quiet')


    def test_rollback_sql_leaves_only_fields_older_versions_know(self):
        # Seed, then a real quiet window more than a day later: the seed
        # marker is still written, and ROLLBACK_SQL removes it.
        import sqlite3
        state, _ = run(EVENING_QUIET + [MORNING_WALK], rest=SLEEP)
        later = MORNING_WALK.end + timedelta(hours=26)
        state, _ = run([hour(later, 0)], state=state)
        self.assertIsNotNone(state.rest_seed_end)
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE detector_state(name TEXT PRIMARY KEY, payload TEXT NOT NULL)')
            db.execute('INSERT INTO detector_state VALUES (?,?)', ('activity', vitals._encoded(state)))
            db.execute(vitals.ROLLBACK_SQL)
            payload = db.execute("SELECT payload FROM detector_state WHERE name='activity'").fetchone()[0]
        self.assertEqual(set(json.loads(payload)), {'mode', 'pending', 'pending_count', 'last_end'})
        self.assertEqual(vitals._decode(ActivityState, payload).mode, state.mode)

    def test_rollback_doc_shows_the_same_sql(self):
        doc = (Path(__file__).resolve().parents[2] / 'pulse_relay/extensions/vitals/README.md').read_text()
        self.assertIn(vitals.ROLLBACK_SQL, doc)

class TickOrderTests(unittest.TestCase):
    def test_tick_passes_sleep_session_to_step_detector(self):
        from unittest.mock import patch
        from pulse_relay.extensions.vitals.activity_windows import PulsStepBucket
        from pulse_relay.extensions.vitals.signal_windows import PulsSleepEvidence
        seen = []
        real = vitals.observe_steps

        def spy(state, window, **kw):
            seen.append(kw.get('rest'))
            return real(state, window, **kw)
        sleep = [PulsSleepEvidence(SLEEP.end, SLEEP.end, 'asleep', 400.0, session_start=SLEEP.start)]
        steps = [PulsStepBucket(MORNING_WALK.start, MORNING_WALK.end, 900.0, MORNING_WALK.end)]
        P = 'pulse_relay.extensions.vitals.tick.'
        with tempfile.TemporaryDirectory() as tmp, \
             patch(P + 'read_puls_step_buckets', return_value=steps), \
             patch(P + 'read_puls_heart_windows', return_value=[]), \
             patch(P + 'read_puls_sleep_evidence', return_value=sleep), \
             patch(P + 'observe_steps', side_effect=spy):
            vitals.vitals_tick(Path(tmp) / 'data', Path(tmp) / 'state',
                               now=MORNING_WALK.end + timedelta(minutes=5))
        self.assertEqual(seen, [SLEEP])

    def test_two_ticks_with_late_sleep_stage_record_one_candidate(self):
        # The same case through the persistent tick and its DB.
        import sqlite3
        from unittest.mock import patch
        from pulse_relay.extensions.vitals.activity_windows import PulsStepBucket
        from pulse_relay.extensions.vitals.signal_windows import PulsSleepEvidence
        P = 'pulse_relay.extensions.vitals.tick.'

        def bucket(w):
            return PulsStepBucket(w.start, w.end, float(w.steps), w.received_at)

        def tick(tmp, steps, sleep, now):
            with patch(P + 'read_puls_step_buckets', return_value=[bucket(w) for w in steps]), \
                 patch(P + 'read_puls_heart_windows', return_value=[]), \
                 patch(P + 'read_puls_sleep_evidence', return_value=sleep):
                return vitals.vitals_tick(Path(tmp) / 'data', Path(tmp) / 'state', now=now)

        def asleep(span):
            return [PulsSleepEvidence(span.end, span.end, 'asleep', 400.0, session_start=span.start)]
        start = MORNING_WALK.end + timedelta(minutes=5)
        short = [StepWindow(start + timedelta(minutes=5 * i), start + timedelta(minutes=5 * (i + 1)),
                            60, start + timedelta(minutes=5 * (i + 1))) for i in range(2)]
        extended = RestSpan(SLEEP.start, SLEEP.end + timedelta(minutes=10))
        with tempfile.TemporaryDirectory() as tmp:
            tick(tmp, EVENING_QUIET, [], EVENING_QUIET[-1].end)                      # bootstrap
            first = tick(tmp, [MORNING_WALK], asleep(SLEEP), MORNING_WALK.end + timedelta(minutes=5))
            second = tick(tmp, short, asleep(extended), short[-1].end + timedelta(minutes=1))
            with sqlite3.connect(Path(tmp) / 'state' / 'state.sqlite3') as db:
                stored = db.execute('SELECT COUNT(*) FROM vitals_candidates').fetchone()[0]
        self.assertEqual((first['new_vitals_candidates'], second['new_vitals_candidates']), (1, 0))
        self.assertEqual(stored, 1)

if __name__ == '__main__':
    unittest.main()
