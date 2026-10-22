"""Minimal Puls-only recent health context; no HTTP endpoint or sender.

Read only the already authenticated Puls ingestion schema; historic HAE
cannot silently contaminate a new Vitals event. Observation time is kept
separate from received time. Null means unknown, never zero.
"""
from __future__ import annotations
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .signal_windows import _rows, _dt, read_puls_sleep_evidence
from .activity_windows import read_puls_step_buckets


@dataclass(frozen=True)
class ContextValue:
    value: float | str
    observed_at: datetime
    source: str = 'pulshealth'


UNITS = {
    'heart_rate': ('count/min','count/minute','bpm'),
    'heart_rate_variability': ('ms','millisecond','milliseconds'),
    'resting_heart_rate': ('count/min','count/minute','bpm'),
}
RANGES = {
    'heart_rate': (30.0,240.0),
    'heart_rate_variability': (0.0,1000.0),
    'resting_heart_rate': (20.0,220.0),
}


def read_puls_context(data_dir: Path, *, now: datetime) -> dict[str, ContextValue]:
    if now.tzinfo is None:
        raise ValueError('timezone-aware now required')
    current=now.astimezone(timezone.utc)
    result: dict[str, ContextValue] = {}
    mappings={'heart_rate':'heart_rate_bpm',
              'heart_rate_variability':'hrv_ms',
              'resting_heart_rate':'resting_heart_rate_bpm'}
    for metric,key in mappings.items():
        allowed=UNITS[metric]
        rows=_rows(data_dir,metric,current-timedelta(hours=26),3000)
        for row in reversed(rows):
            if row['kind']!='quantity' or row['units'].strip().lower() not in allowed:
                continue
            observed=_dt(row['observed_at'])
            if observed is None or observed>current:continue
            try:
                value=json.loads(row['payload_json']).get('qty')
            except (TypeError,ValueError,AttributeError):continue
            low,high=RANGES[metric]
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or not low<=value<=high:continue
            result[key]=ContextValue(float(value),observed)
            break
    # An hour of steps must be an hour's worth of complete, all-device
    # coverage, not one 5-minute bucket mislabeled 'last_hour'.
    items=read_puls_step_buckets(data_dir,now=current,lookback_hours=3)
    start=current-timedelta(minutes=60)
    relevant=[item for item in items if item.start>=start and item.end<=current]
    coverage=sum((item.end-item.start).total_seconds()/60 for item in relevant)
    if (coverage>=45 and relevant and
            current-max(item.end for item in relevant)<=timedelta(minutes=20)):
        result['steps_last_hour']=ContextValue(sum(item.steps for item in relevant),max(item.end for item in relevant))
    # Do NOT claim an old sleep session is the owner's CURRENT state.
    # Only a recent, explicitly confirmed wake supplies awake context.
    evidence=read_puls_sleep_evidence(data_dir,now=current)
    if evidence and evidence[-1].state=='awake' and current-evidence[-1].observed_at<=timedelta(minutes=60):
        result['sleep_state']=ContextValue('awake',evidence[-1].observed_at)
    return result
