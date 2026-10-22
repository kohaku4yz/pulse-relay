# Vitals detectors: algorithm

How the Vitals extension turns PulsHealth data into coarse "something
changed" signals. The code next to this file is the source of truth. Install,
timer and privacy operations are in `pulse_relay/extensions/vitals/README.md`.

This is a companionship / change-notification feature. It is not a clinical
monitor, an emergency alarm or a diagnosis.

## Pipeline

1. **Readers** (`activity_windows.py`, `signal_windows.py`) build
   complete, de-duplicated, PulsHealth-only windows from the Health Service
   store: all-device step aggregates, short heart-rate windows (median of
   source-verified readings) and normalized sleep-session evidence. Legacy
   HAE rows, single-device Watch/iPhone duplicates and daily totals are
   excluded.
2. **Detectors** (`activity_transitions_core.py`,
   `multisignal_core.py`) are pure state machines. They take one
   observation and the previous state and return the new state plus at most
   one transition. They do no I/O.
3. **Persistent tick** (`tick.py`) runs as a one-shot every
   five minutes via the `pulse-relay-vitals` systemd timer. It loads the
   detector states from the private Vitals state database, feeds the recent windows
   (3 h of steps and heart rate, 36 h of sleep evidence) through the
   detectors, keeps only changes that are still fresh relative to the tick
   time, and groups changes within 10 minutes of each other into one
   `life.vitals.changed` candidate with a list of trigger names. Detector
   states and candidates are committed in one transaction. The candidate ID
   is a hash of its content, so replays never duplicate it. The first run
   is a silent bootstrap: it learns the current state and records nothing.
4. **Outbox** (`outbox.py`, served at the loopback-only
   `GET /v1/health/vitals-outbox`) returns recent candidates (anything older
   than 90 minutes is dropped). When at least one candidate exists, it adds a
   bounded context snapshot read at request time (`context_snapshot.py`).
5. **Delivery** is done by the separate events service. It polls the outbox,
   validates every field again, and delivers each candidate to the AI agent
   inside one context event (triggers plus snapshot). This service never
   pushes anything outbound and the events service never gets database
   access.

## Signals

| Channel   | Transitions                              | Detector         |
|-----------|------------------------------------------|------------------|
| Steps     | `movement_started`, `movement_settled`   | `observe_steps`  |
| Heart rate| `heart_rate_rose`, `heart_rate_fell`     | `observe_heart`  |
| Sleep     | `sleep_started`, `woke_up`               | `observe_sleep`  |

Any one confirmed change can wake the agent. Changes that are confirmed close
together go out as one event with several triggers, not one event per metric.

### Step activity (`observe_steps`)

States: `unknown`, `quiet`, `moving`.

- A window is labelled *moving* at >= 6 steps/min and *quiet* at <= 1
  step/min. Rates in between are ambiguous: they produce no label and clear
  any pending confirmation.
- A change needs `confirm_windows` (2) consecutive short windows with the
  same new label. A complete window of >= 30 minutes already counts as
  sustained evidence and confirms on its own.
- `unknown -> quiet/moving` only sets the baseline and emits nothing.
  `quiet -> moving` emits `movement_started`. `moving -> quiet` emits
  `movement_settled`.
- Accepted window length is 5-60 minutes, so daily aggregates are ignored.
  Windows must be complete.
- Freshness: the window may be at most `duration + 20` minutes old relative
  to the `now` passed in. The tick passes the later of window end and server
  receipt time, then checks freshness against the tick time separately.
- A window that does not directly follow the previous one (gap or overlap
  over 60 s) resets the state to `unknown`. A missing window is never
  evidence of rest.
- **Exception: a gap covered by confirmed sleep.** If the latest main sleep
  session (see Sleep) started no later than 60 minutes after the last
  bucket before the gap, and ended no earlier than 60 minutes before the
  first bucket after it, the state becomes `quiet` instead of `unknown`.
  The evidence of rest is the sleep stages, not the missing steps. The
  first confirmed `moving` after waking then emits `movement_started`.
  - At most one seed per sleep session, so later gaps that morning reset
    to `unknown` as usual.
  - A seeded `quiet` that no real quiet window has confirmed expires 4 h
    after the session ends; a walk later than that is a silent baseline
    again, not "the first movement after waking".
  - The tick reads sleep evidence before steps so the session is known
    when the morning buckets are processed. If sleep data arrives after
    those buckets, the morning stays silent (no backfill).
- A window that ends at or before the last processed one (replay or
  retroactive correction) is ignored.

### Heart rate (`observe_heart`)

- Input: the median of >= 3 readings in a complete 3-15 minute window.
- The anchor is the last confirmed level. A window counts as a change when
  it differs from the anchor by at least `max(15 bpm, 18% of anchor)`.
- Two consecutive windows in the same direction confirm a change, emit
  `heart_rate_rose` / `heart_rate_fell` and move the anchor. A single
  spike, or a window back inside the band, clears the pending count.
- Windows more than 30 minutes old are rejected. A gap over 10 minutes
  silently resets the anchor. The next window becomes the new baseline
  and no event is emitted.
- A raised wrist pulse alone does not prove walking, stress or illness.

### Sleep (`observe_sleep`)

- Input is normalized session evidence, not raw stages. The reader emits
  *asleep* only after sustained explicit sleep-stage coverage, and *awake*
  only after >= 30 min of explicit awake stage following the main session.
  A brief HealthKit "awake" stage, or simply having no sleep samples, is
  never treated as waking up.
- The detector also requires `complete` and `main_session_evidence`, at
  least 20 min confirmed for asleep and 30 min for awake, and a lag of at
  most 90 minutes.
- `unknown -> asleep/awake` sets the baseline only. An observation more than
  18 h after the previous one starts a new unknown session instead of
  making up a wake.

## Safety principles

- **Complete windows only.** Partial buckets, daily totals, mixed sources and
  invalid values (NaN, negative, out of range) are rejected at construction
  or ignored.
- **Confirmation before change.** Every channel needs sustained evidence.
  The first observation and every post-gap rebaseline are silent.
- **Silent rebaseline after gaps.** Missing data means unknown. It is never
  read as inactivity, sleep or a fabricated transition. Only explicit
  sleep-stage evidence covering a gap can stand in for rest (see Step
  activity).
- **No backfill as news.** Old syncs and delayed uploads never cause a
  "just now" event. Replays never repeat an event.
- **No raw values in event payloads.** Candidates carry only trigger names
  and timestamps. The context snapshot holds bounded summaries, each with
  its own observation time. Stale or absent metrics are left out, never
  reported as zero or as current. No GPS, device IDs, HealthKit UUIDs, raw
  time series or free-text medical notes.
- **Neutral, non-clinical language.** Thresholds are experimental
  sensitivity settings, not medical limits. There are no fixed quiet hours,
  bedtimes, personal baselines or blanket cooldowns.

## Known limitations (tuning candidates)

- **Poll cadence affects acceptance.** The stale-data gate measures
  `now - window end`, so how often the tick and the outbox poll run decides
  whether a borderline change is still accepted.
- **`sleep_started` effectively never fires live.** Overnight data typically
  arrives only after the first phone unlock, and the 90-minute sleep lag
  gate rejects it by then.
- **The first movement after a night is sometimes still missed.** A gap
  covered by confirmed sleep now seeds `quiet` (see Step activity). In a
  multi-night replay (hourly buckets, ideal arrival) the first walk after
  waking was reported far more
  often than before. It stays silent when another gap comes
  before the first moving hour, or when sleep data arrives after the
  morning buckets.
- **Daytime gaps still reset activity.** Zero-step hours are not stored, so
  a quiet hour during the day is a missing bucket and resets to `unknown`.
  This loses more transitions than the overnight gap did.
- **One walk can produce two events.** Heart-rate and step signals arrive
  with different delays, so their transitions can land in different ticks
  or more than 10 minutes apart, which gives two separate events.

## Tests

`tests/vitals/test_activity_transitions_core.py` and
`tests/vitals/test_multisignal_core.py` (stdlib `unittest`,
synthetic UTC fixtures only).
