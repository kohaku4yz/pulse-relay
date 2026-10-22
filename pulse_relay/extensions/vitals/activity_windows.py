"""Read-only, source-verified PulsHealth step aggregates for activity transitions.

Called by the persistent Vitals on its timer; no HTTP endpoint of its own
and no outbound notifications.
Run only within the existing Health Service's authorized data boundary.
Legacy HAE entries and individual Watch/iPhone raw data are excluded.
"""
from __future__ import annotations
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class PulsStepBucket:
    start: datetime
    end: datetime
    steps: float
    received_at: datetime
    complete: bool = True
    source: str = "pulshealth"
    device_filter: str = "all"


def _date(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if result.tzinfo is None:
        return None
    return result.astimezone(timezone.utc)


def read_puls_step_buckets(data_dir: Path, *, now: datetime,
                           lookback_hours: int = 8, limit: int = 240
                           ) -> list[PulsStepBucket]:
    """Return short, complete, *all-device* PulsHealth aggregate buckets.

    Deliberately refuse daily aggregates and raw quantity samples: they
    cannot support short state transitions or safe device deduplication.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone aware")
    if not 1 <= lookback_hours <= 48 or not 1 <= limit <= 500:
        raise ValueError("invalid reader bounds")
    db_path = data_dir / "health.sqlite3"
    if not db_path.is_file():
        return []
    start_cutoff = (now.astimezone(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    # An unreadable store raises: an empty result would look like "no
    # activity" and leave the detectors silently blind.
    connection = sqlite3.connect(f"file:{db_path}?mode=ro",uri=True)
    connection.row_factory = sqlite3.Row
    try:
        # PulsHealth aggregates are generated only by the authenticated
        # ingest_puls_ndjson parser with a fixed provenance source label.
        rows = connection.execute(
            """
            SELECT observed_at, source, units, kind, payload_json, received_at
            FROM metric_samples
            WHERE metric='step_count' AND kind='aggregate'
              AND source LIKE 'PulsHealth aggregate:all:%'
              AND observed_at >= ?
            ORDER BY observed_at ASC
            LIMIT ?
            """,
            (start_cutoff.replace("+00:00","Z"), limit),
        ).fetchall()
    finally:
        connection.close()

    buckets: list[PulsStepBucket] = []
    for row in rows:
        if row["units"] not in ("count", "counts", ""):
            continue
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict) or payload.get("func")!="sum" or payload.get("device_filter")!="all":
            continue
        interval=payload.get("interval_value")
        unit=payload.get("interval_unit")
        if isinstance(interval,bool) or not isinstance(interval,int) or interval<1 or unit not in ("minute","hour"):
            continue
        begin=_date(row["observed_at"])
        end=_date(payload.get("bucket_end"))
        received=_date(row["received_at"])
        qty=payload.get("qty")
        if (begin is None or end is None or received is None
            or isinstance(qty,bool) or not isinstance(qty,(int,float))
            or not math.isfinite(float(qty)) or qty<0):
            continue
        duration=(end-begin).total_seconds()/60.0
        expected=interval*(60 if unit=="hour" else 1)
        if not (5<=duration<=60 and abs(duration-expected)<0.001):
            continue
        if end>now.astimezone(timezone.utc):
            continue  # partial or future bucket
        buckets.append(PulsStepBucket(begin,end,float(qty),received))
    # Avoid overlapping aggregations if e.g. 5min + 1hour configured at once.
    # The narrowest supported bucket wins for the same period; gaps are
    # tolerated by the later activity detector as rebaseline-only intervals.
    ordered=sorted(buckets,key=lambda b: (b.end, b.start))
    shortest=sorted(ordered,key=lambda b: (b.end-b.start,b.end))
    selected=[]
    for bucket in shortest:
        if any(bucket.start<old.end and bucket.end>old.start for old in selected):
            continue
        selected.append(bucket)
    return sorted(selected,key=lambda b:b.end)


__all__ = ["PulsStepBucket", "read_puls_step_buckets"]
