"""PulsHealth-only heart and sleep evidence: READ ONLY, never a network API.

Source provenance is established by the Puls ingest UUID map, NOT by the
human-readable Watch/iPhone device name. No HAE records can enter this reader.
"""
from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Literal


def _dt(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return result.astimezone(timezone.utc) if result.tzinfo else None


def _rows(data_dir: Path, metric: str, since: datetime, limit: int) -> list[sqlite3.Row]:
    path = data_dir / 'health.sqlite3'
    if not path.is_file():
        return []
    # An unreadable store raises: an empty result would look like "no data"
    # and leave the detectors silently blind.
    with sqlite3.connect(f'file:{path}?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        return db.execute('''
            SELECT s.observed_at, s.source, s.units, s.kind,
                   s.payload_json, s.received_at
            FROM metric_samples s
            JOIN puls_sample_map p
            ON s.metric=p.metric AND s.observed_at=p.observed_at
               AND s.source=p.source
            WHERE s.metric=? AND s.observed_at>=?
            ORDER BY s.observed_at ASC LIMIT ?''',
            (metric, since.isoformat().replace('+00:00','Z'),limit)).fetchall()


@dataclass(frozen=True)
class PulsHeartBucket:
    start: datetime
    end: datetime
    median_bpm: float
    samples: int
    received_at: datetime
    source: str = 'pulshealth'


def read_puls_heart_windows(data_dir: Path, *, now: datetime,
                            window_minutes: int = 15,
                            lookback_hours: int = 3) -> list[PulsHeartBucket]:
    if now.tzinfo is None or window_minutes not in (5, 10, 15) or not 1 <= lookback_hours <= 24:
        raise ValueError('invalid heart reader configuration')
    current = now.astimezone(timezone.utc)
    since = current - timedelta(hours=lookback_hours)
    grouped: dict[datetime, list[tuple[float, datetime, str, datetime]]] = {}
    for row in _rows(data_dir,'heart_rate',since,8000):
        if row['kind'] != 'quantity' or str(row['units']).strip().lower() not in ('count/min','bpm','count/minute'):
            continue
        observed, received = _dt(row['observed_at']), _dt(row['received_at'])
        if observed is None or received is None or observed>current:
            continue
        try:
            value=json.loads(row['payload_json']).get('qty')
        except (ValueError, TypeError, AttributeError):
            continue
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or not 30 <= value <= 240:
            continue
        window_secs=window_minutes*60
        start=datetime.fromtimestamp(int(observed.timestamp())//window_secs*window_secs,timezone.utc)
        grouped.setdefault(start,[]).append((float(value),received,str(row['source']),observed))
    out=[]
    for start,rows in sorted(grouped.items()):
        end=start+timedelta(minutes=window_minutes)
        if end>current:continue
        # Avoid weighting duplicate sources at the same timestamp. Prefer
        # Watch series only when it actually has >=3 independent samples.
        watch=[r for r in rows if 'apple watch' in r[2].casefold()]
        rows=watch if len({r[3] for r in watch})>=3 else rows
        unique={stamp:(value,received) for value,received,_,stamp in rows}
        if len(unique)<3:continue
        out.append(PulsHeartBucket(start,end,float(median(v[0] for v in unique.values())),len(unique),max(v[1] for v in unique.values())))
    return out


@dataclass(frozen=True)
class PulsSleepEvidence:
    observed_at: datetime
    received_at: datetime
    state: Literal['asleep','awake']
    confirmed_minutes: float
    main_session_evidence: bool = True
    source: str = 'pulshealth'
    # Start of the main session (asleep evidence only), so a step gap can be
    # checked against the whole session rather than just its end.
    session_start: datetime | None = None


def read_puls_sleep_evidence(data_dir: Path, *, now: datetime,
                             lookback_hours: int = 36) -> list[PulsSleepEvidence]:
    """Conservative proof, NOT guessed sleep/wake from lack of samples.

    Emit asleep evidence after >=2h of explicit sleep-stage coverage.
    Emit awake evidence only after >=30min of explicit awake stage AFTER
    this main session, without later overlapping sleep. This will miss many
    real awakenings, which is safer than treating midnight arousals as wake.
    """
    if now.tzinfo is None or not 3 <= lookback_hours <= 72:
        raise ValueError('invalid sleep reader configuration')
    current=now.astimezone(timezone.utc)
    items=[]
    for row in _rows(data_dir,'sleep_analysis',current-timedelta(hours=lookback_hours),10000):
        if row['kind']!='sleep_segment':continue
        try:
            content=json.loads(row['payload_json'])
        except (ValueError,TypeError):continue
        if not isinstance(content,dict):continue
        start,end,received=_dt(content.get('start')),_dt(content.get('end')),_dt(row['received_at'])
        if start is None or end is None or received is None or end<=start or end>current or end-start>timedelta(hours=24):continue
        if content.get('stage') not in ('core','deep','rem','asleep','awake'):continue
        items.append((start,end,content['stage'],received,str(row['source'])))
    if not items:return []
    # Do not combine two devices' overlapping sessions: choose source with
    # greatest primary sleep coverage within the lookback.
    sums={}
    for start,end,stage,_,source in items:
        if stage!='awake':sums[source]=sums.get(source,0)+(end-start).total_seconds()
    if not sums:return []
    preferred=max(sums,key=sums.get)
    items=sorted([r for r in items if r[4]==preferred],key=lambda r:r[0])
    sleeps=[r for r in items if r[2]!='awake']
    # Avoid duplicating generic asleep and detailed stages: union intervals.
    merged=[]
    for start,end,*_ in sorted(sleeps,key=lambda r:r[0]):
        if merged and start <= merged[-1][1]:
            merged[-1]=(merged[-1][0], max(merged[-1][1],end))
        else:merged.append((start,end))
    if not merged:return []
    # Take the latest contiguous main sleep session; small stage gaps are okay.
    sessions=[]
    running=[]
    for interval in merged:
        if running and interval[0]-running[-1][1] > timedelta(minutes=30):
            sessions.append(running);running=[]
        running.append(interval)
    if running:sessions.append(running)
    primary=next((group for group in reversed(sessions)
                 if sum((e-s).total_seconds() for s,e in group)>=2*3600),None)
    if primary is None:return []
    sleep_end=max(end for _,end in primary)
    confirmed=sum((e-s).total_seconds() for s,e in primary)/60
    received=max(r[3] for r in sleeps if r[1]<=sleep_end)
    session_start=min(start for start,_ in primary)
    result=[PulsSleepEvidence(sleep_end,received,'asleep',confirmed,
                              session_start=session_start)]
    # Explicit 30m awake AFTER last sleep segment of the primary session.
    awake=[r for r in items if r[2]=='awake' and r[0]>=sleep_end and r[1]>r[0]]
    awake.sort(key=lambda r:r[0])
    if awake:
        earliest=awake[0][0]; latest=earliest; durations=0.0; receipts=[]
        for begin,end,_,rtime,_ in awake:
            if begin>latest+timedelta(minutes=2):break
            overlap=max(begin,latest)
            durations+=(end-overlap).total_seconds()/60
            latest=max(latest,end);receipts.append(rtime)
        if durations>=30 and not any(s<latest and e>sleep_end for s,e in merged):
            result.append(PulsSleepEvidence(latest,max(receipts),'awake',durations))
    return result
