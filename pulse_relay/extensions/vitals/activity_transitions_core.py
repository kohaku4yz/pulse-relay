"""PulsHealth-only incremental *activity changes*, not daily wellness deviations.

Pure decision logic: no DB reads, event delivery or subscriptions. The live
caller (tick.py) feeds it complete, non-overlapping all-device
step windows built by activity_windows.py from authenticated Puls aggregates.

A missing window is UNKNOWN, never evidence that the owner is resting.
The one exception is a gap covered by a confirmed main sleep session: sleep
evidence (not missing data) shows rest, so the first walk after waking can
be reported. An initial mode only establishes state; an actual transition
is notified.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Literal

Mode = Literal["unknown", "quiet", "moving"]
Reason = Literal["movement_started", "movement_settled"]


@dataclass(frozen=True)
class StepWindow:
    start: datetime
    end: datetime
    steps: float
    received_at: datetime
    source: str = "pulshealth"
    device_filter: str = "all"
    complete: bool = True

    def __post_init__(self):
        for timestamp in (self.start, self.end, self.received_at):
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                raise ValueError("timestamps must be timezone aware")
        if self.end <= self.start:
            raise ValueError("empty or negative sample interval")
        if self.source != "pulshealth" or self.device_filter != "all":
            raise ValueError("only PulsHealth all-device windows accepted")
        if isinstance(self.steps, bool) or not isinstance(self.steps, (int, float)):
            raise ValueError("invalid step sample")
        if not math.isfinite(float(self.steps)) or self.steps < 0:
            raise ValueError("invalid step sample")

    @property
    def duration_min(self) -> float:
        return (self.end - self.start).total_seconds() / 60.0

    @property
    def steps_per_min(self) -> float:
        return float(self.steps) / self.duration_min


@dataclass(frozen=True)
class ActivityConfig:
    # Configurable hypotheses, NOT universal physiology or notification schedule.
    enter_moving_steps_min: float = 6.0
    enter_quiet_steps_min: float = 1.0
    confirm_windows: int = 2
    # An hour of complete aggregate evidence is already sustained;
    # don't force two additional hours after a proven prior mode.
    long_window_one_confirm_minutes: int = 30
    max_window_minutes: int = 60
    min_window_minutes: int = 5
    freshness_grace_minutes: int = 20
    # A sleep session "covers" a step gap if it starts no later than this
    # after the last bucket before the gap and ends no earlier than this
    # before the first bucket after it (one hourly bucket of slack).
    rest_edge_tolerance_minutes: int = 60
    # A quiet mode seeded from sleep that no real quiet window has confirmed
    # expires this long after the session ends: a walk much later is not
    # "the first movement after waking".
    rest_seed_max_after_wake_minutes: int = 240


@dataclass(frozen=True)
class RestSpan:
    """A confirmed main sleep session (start, end) from sleep-stage evidence."""
    start: datetime
    end: datetime

    def __post_init__(self):
        for timestamp in (self.start, self.end):
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                raise ValueError("timestamps must be timezone aware")
        if self.end <= self.start:
            raise ValueError("empty or negative rest span")


@dataclass(frozen=True)
class ActivityState:
    mode: Mode = "unknown"
    pending: Literal["quiet", "moving", ""] = ""
    pending_count: int = 0
    last_end: datetime | None = None
    # Span of the sleep session that last seeded quiet. A later RestSpan that
    # overlaps it is the same session (a late stage can move its end, a
    # correction its start), so each session seeds at most once.
    rest_seed_start: datetime | None = None
    rest_seed_end: datetime | None = None
    # Set while quiet comes only from a sleep seed; cleared once a real
    # quiet window confirms it or a transition happens.
    seed_until: datetime | None = None


@dataclass(frozen=True)
class ActivityTransition:
    reason: Reason
    observed_at: datetime
    from_mode: Literal["quiet", "moving"]
    to_mode: Literal["quiet", "moving"]
    evidence_window_min: float
    # No steps, heart rate, or personal health metrics in outbound form.


def _window_label(window: StepWindow, config: ActivityConfig) -> Literal["quiet", "moving", ""]:
    rate = window.steps_per_min
    if rate >= config.enter_moving_steps_min:
        return "moving"
    if rate <= config.enter_quiet_steps_min:
        return "quiet"
    return ""


def _keep_seed_marker(state: ActivityState) -> ActivityState:
    """A fresh unknown state that still remembers which session seeded."""
    return ActivityState(rest_seed_start=state.rest_seed_start,
                         rest_seed_end=state.rest_seed_end)


def _same_session(state: ActivityState, rest: RestSpan) -> bool:
    if state.rest_seed_start is None or state.rest_seed_end is None:
        return False
    return rest.start <= state.rest_seed_end and rest.end >= state.rest_seed_start


def _after_gap(state: ActivityState, last: datetime, window: StepWindow,
               rest: RestSpan | None, config: ActivityConfig) -> ActivityState:
    """State after a gap: unknown, unless confirmed sleep covers the gap."""
    unknown = _keep_seed_marker(state)
    if rest is None or window.start <= last or _same_session(state, rest):
        return unknown
    slack = timedelta(minutes=config.rest_edge_tolerance_minutes)
    if rest.start > last + slack or rest.end < window.start - slack:
        return unknown
    return ActivityState(
        mode="quiet", rest_seed_start=rest.start, rest_seed_end=rest.end,
        seed_until=rest.end + timedelta(minutes=config.rest_seed_max_after_wake_minutes),
    )


def observe_steps(
    state: ActivityState,
    window: StepWindow,
    *,
    now: datetime,
    config: ActivityConfig = ActivityConfig(),
    rest: RestSpan | None = None,
) -> tuple[ActivityState, ActivityTransition | None]:
    """Observe one completed, timely aggregate step interval.

    Never publish an event for first observation or post-gap rebaseline,
    except that a gap covered by ``rest`` (a confirmed main sleep session)
    rebaselines to quiet, once per session. Two consecutive qualified
    windows confirm a change; no bedtime, global 6-hour cooldown, or static
    time-of-day policy.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone aware")
    if not (0 <= config.enter_quiet_steps_min < config.enter_moving_steps_min
            and config.confirm_windows >= 1 and config.long_window_one_confirm_minutes > 0
            and config.min_window_minutes <= config.max_window_minutes
            and config.rest_edge_tolerance_minutes >= 0
            and config.rest_seed_max_after_wake_minutes > 0):
        raise ValueError("invalid activity configuration")
    if state.last_end is not None and window.end <= state.last_end:
        return state, None  # Replay or retrospective corrections do not re-notify.
    if not window.complete:
        return state, None
    if not (config.min_window_minutes <= window.duration_min <= config.max_window_minutes):
        return state, None
    lag = (now.astimezone(timezone.utc) - window.end.astimezone(timezone.utc)).total_seconds() / 60
    # Hourly aggregate can legitimately settle later than a five-minute one.
    allowed_lag = window.duration_min + config.freshness_grace_minutes
    if lag < -2 or lag > allowed_lag:
        return state, None  # Old sync/backfill cannot cause "just now" event.
    last = state.last_end
    if last is not None:
        gap = (window.start.astimezone(timezone.utc) - last.astimezone(timezone.utc)).total_seconds()
        if abs(gap) > 60:  # Neither overlapping nor disjoint time buckets are comparable.
            state = _after_gap(state, last, window, rest, config)
    if state.seed_until is not None and window.end > state.seed_until:
        # Unconfirmed sleep seed is too old to explain this window.
        state = _keep_seed_marker(state)

    label = _window_label(window, config)
    if not label:
        return replace(state, pending="", pending_count=0, last_end=window.end), None
    if label == state.mode:
        # A real quiet window confirms a seeded quiet mode.
        return replace(state, pending="", pending_count=0, last_end=window.end,
                       seed_until=None), None

    count = state.pending_count + 1 if state.pending == label else 1
    required = (1 if window.duration_min >= config.long_window_one_confirm_minutes
                else config.confirm_windows)
    if count < required:
        return replace(state, pending=label, pending_count=count, last_end=window.end), None

    previous = state.mode
    accepted = replace(_keep_seed_marker(state), mode=label, last_end=window.end)
    if previous == "unknown":
        return accepted, None  # First observed mode does NOT prove transition.
    return accepted, ActivityTransition(
        reason="movement_started" if label == "moving" else "movement_settled",
        observed_at=window.end, from_mode=previous, to_mode=label,
        evidence_window_min=window.duration_min * required,
    )


__all__ = [
    "StepWindow", "ActivityConfig", "RestSpan", "ActivityState", "ActivityTransition",
    "observe_steps",
]
