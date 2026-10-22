"""Pure PulsHealth activity/heart/sleep transition recognizers.

No owner data is fetched and no Webhook is sent by this module. Ingest adapters
are trusted to supply completed, de-duplicated, source-proven observations.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

Signal = Literal[
    "movement_started", "movement_settled",
    "heart_rate_rose", "heart_rate_fell",
    "sleep_started", "woke_up",
]


def _utc(t: datetime) -> datetime:
    if not isinstance(t, datetime) or t.tzinfo is None or t.utcoffset() is None:
        raise ValueError("timezone-aware timestamp required")
    return t.astimezone(timezone.utc)


@dataclass(frozen=True)
class HeartWindow:
    """A complete, non-overlapping window summarized from Puls raw HR UUIDs."""
    start: datetime
    end: datetime
    median_bpm: float
    samples: int
    received_at: datetime
    source: str = "pulshealth"

    def __post_init__(self) -> None:
        if not _utc(self.start) < _utc(self.end):
            raise ValueError("nonpositive heart interval")
        _utc(self.received_at)
        if self.source != "pulshealth":
            raise ValueError("non-Puls data")
        if (isinstance(self.samples, bool) or not isinstance(self.samples, int)
                or self.samples < 0):
            raise ValueError("bad sample count")
        if (isinstance(self.median_bpm, bool)
                or not isinstance(self.median_bpm, (int, float))
                or not math.isfinite(self.median_bpm)
                or not 30 <= self.median_bpm <= 240):
            raise ValueError("invalid heart window")


@dataclass(frozen=True)
class HeartConfig:
    min_samples: int = 3
    min_window_minutes: float = 3.0
    max_window_minutes: float = 15.0
    absolute_change_bpm: float = 15.0
    relative_change: float = 0.18
    confirm_windows: int = 2
    max_lag_minutes: float = 30.0
    max_gap_minutes: float = 10.0


@dataclass(frozen=True)
class HeartState:
    anchor_bpm: float | None = None
    pending: Literal["heart_rate_rose", "heart_rate_fell", ""] = ""
    pending_count: int = 0
    last_end: datetime | None = None


@dataclass(frozen=True)
class SignalChange:
    signal: Signal
    observed_at: datetime
    # Private per-detection metadata is NOT the outbound MCP payload.


def observe_heart(
    state: HeartState, window: HeartWindow, *, now: datetime,
    config: HeartConfig = HeartConfig(),
) -> tuple[HeartState, SignalChange | None]:
    """A sustained change relative to the last confirmed anchor, NOT a diagnosis."""
    if config.min_samples < 1 or config.confirm_windows < 1 or config.absolute_change_bpm <= 0:
        raise ValueError("invalid heart config")
    observed = _utc(window.end)
    duration = (_utc(window.end)-_utc(window.start)).total_seconds()/60
    if (window.samples < config.min_samples
        or not config.min_window_minutes <= duration <= config.max_window_minutes):
        return state, None
    lag = (_utc(now) - observed).total_seconds()/60
    if lag < -2 or lag > config.max_lag_minutes:
        return state, None  # Backfill is not a live signal.
    if state.last_end is not None:
        if observed <= _utc(state.last_end):
            return state, None
        gap = (_utc(window.start)-_utc(state.last_end)).total_seconds()/60
        if gap < -1 or gap > config.max_gap_minutes:
            state = HeartState()  # Re-establish baseline without a wake-up.
    if state.anchor_bpm is None:
        return HeartState(float(window.median_bpm), last_end=window.end), None
    difference = float(window.median_bpm) - state.anchor_bpm
    bound = max(config.absolute_change_bpm,
                abs(state.anchor_bpm)*config.relative_change)
    direction: Literal["heart_rate_rose","heart_rate_fell",""]
    direction = "heart_rate_rose" if difference >= bound else (
        "heart_rate_fell" if difference <= -bound else "")
    if not direction:
        return HeartState(state.anchor_bpm, "", 0, window.end), None
    count = state.pending_count+1 if state.pending == direction else 1
    if count < config.confirm_windows:
        return HeartState(state.anchor_bpm,direction,count,window.end),None
    return HeartState(float(window.median_bpm),last_end=window.end), SignalChange(
        direction,observed
    )


@dataclass(frozen=True)
class SleepObservation:
    """Trustworthy *normalized session evidence*, not a raw brief awake stage.

    Future Health Service normalizer must avoid interpreting any single
    awake category/absence of sleep segments as 'woke_up'.
    """
    observed_at: datetime
    received_at: datetime
    state: Literal["asleep", "awake"]
    confirmed_minutes: float
    source: str = "pulshealth"
    complete: bool = True
    main_session_evidence: bool = True

    def __post_init__(self) -> None:
        _utc(self.observed_at)
        _utc(self.received_at)
        if self.source != "pulshealth":
            raise ValueError("non-Puls sleep input")
        if (not isinstance(self.confirmed_minutes,(int,float))
            or isinstance(self.confirmed_minutes,bool)
            or not math.isfinite(self.confirmed_minutes)
            or self.confirmed_minutes < 0):
            raise ValueError("invalid sleep confirmation")


@dataclass(frozen=True)
class SleepState:
    mode: Literal["unknown", "asleep", "awake"] = "unknown"
    last_observed_at: datetime | None = None


@dataclass(frozen=True)
class SleepConfig:
    min_asleep_minutes: float = 20.0
    min_awake_minutes: float = 30.0
    max_lag_minutes: float = 90.0
    max_gap_hours: float = 18.0


def observe_sleep(
    state: SleepState, item: SleepObservation, *, now: datetime,
    config: SleepConfig = SleepConfig(),
) -> tuple[SleepState, SignalChange | None]:
    if not item.complete or not item.main_session_evidence:
        return state, None
    if item.confirmed_minutes < (
        config.min_asleep_minutes if item.state == "asleep"
        else config.min_awake_minutes
    ):
        return state, None
    lag = (_utc(now) - _utc(item.observed_at)).total_seconds()/60
    if lag < -2 or lag > config.max_lag_minutes:
        return state,None
    if state.last_observed_at is not None:
        if _utc(item.observed_at) <= _utc(state.last_observed_at):
            return state,None
        if (_utc(item.observed_at)-_utc(state.last_observed_at)) > timedelta(
            hours=config.max_gap_hours
        ):
            state = SleepState()  # A new unknown session, not a fabricated wake.
    if item.state == state.mode:
        return SleepState(item.state,item.observed_at),None
    prev = state.mode
    updated = SleepState(item.state,item.observed_at)
    if prev == "unknown":
        return updated,None
    return updated,SignalChange(
        "sleep_started" if item.state == "asleep" else "woke_up",
        _utc(item.observed_at)
    )
