"""Test-only writer for legacy Health Auto Export (HAE) rows.

The service no longer accepts HAE uploads (the route answers 410), but stores that
were fed by HAE keep those rows as history, and the readers must still serve them.
This module reproduces the retired writer so tests can create such historical rows.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pulse_relay.store import (
    _canonical_metric,
    _canonical_source,
    _iso_utc,
    _number,
    _open_write,
    _safe_text,
    _utc_now,
)
from datetime import timezone

MAX_METRIC_GROUPS = 128
MAX_RECORDS_PER_GROUP = 10_000
MAX_TOTAL_RECORDS = 25_000


class HealthPayloadError(ValueError):
    """Raised when a legacy HAE payload cannot be interpreted."""


def _parse_timestamp(value: object) -> tuple[str, str]:
    if not isinstance(value, str) or not value.strip():
        raise HealthPayloadError("record date must be a non-empty string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise HealthPayloadError("record date is not ISO-8601 compatible") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return _iso_utc(parsed), parsed.date().isoformat()


def _range_payload(record: dict[str, Any]) -> dict[str, float | int] | None:
    values: dict[str, float | int] = {}
    for output_key, candidates in (
        ("min", ("Min", "min")),
        ("avg", ("Avg", "avg", "average")),
        ("max", ("Max", "max")),
    ):
        for candidate in candidates:
            parsed = _number(record.get(candidate))
            if parsed is not None:
                values[output_key] = parsed
                break
    return values or None


def _sleep_payload(record: dict[str, Any]) -> dict[str, Any] | None:
    payload: dict[str, Any] = {}
    for key, output_key in (
        ("core", "core"),
        ("rem", "rem"),
        ("deep", "deep"),
        ("awake", "awake"),
        ("inBed", "in_bed"),
    ):
        value = _number(record.get(key))
        if value is not None:
            payload[output_key] = value

    for key, output_key in (
        ("inBedStart", "in_bed_start"),
        ("inBedEnd", "in_bed_end"),
        ("sleepStart", "sleep_start"),
        ("sleepEnd", "sleep_end"),
    ):
        raw = record.get(key)
        if isinstance(raw, str) and raw.strip():
            try:
                payload[output_key] = _parse_timestamp(raw)[0]
            except HealthPayloadError:
                pass

    # Tiny fallback for non-aggregated sleep segments; arbitrary metadata is
    # deliberately not retained.
    stage = record.get("value")
    start = record.get("startDate")
    end = record.get("endDate")
    if not payload and isinstance(stage, str) and isinstance(start, str):
        try:
            payload["stage"] = stage[:48]
            payload["start"] = _parse_timestamp(start)[0]
            if isinstance(end, str):
                payload["end"] = _parse_timestamp(end)[0]
        except HealthPayloadError:
            return None

    return payload or None


def _normalize_record(
    metric: str,
    units: str,
    record: object,
) -> tuple[str, str, str, str, str] | None:
    if not isinstance(record, dict):
        return None

    raw_date = record.get("date")
    if raw_date is None and metric == "sleep_analysis":
        raw_date = record.get("startDate")
    try:
        observed_at, local_date = _parse_timestamp(raw_date)
    except HealthPayloadError:
        return None

    source = _canonical_source(record.get("source"))

    if metric == "sleep_analysis":
        payload = _sleep_payload(record)
        kind = "sleep"
    elif metric == "heart_rate":
        payload = _range_payload(record)
        if payload is not None:
            kind = "range"
        else:
            qty = _number(record.get("qty"))
            payload = {"qty": qty} if qty is not None else None
            kind = "quantity"
    else:
        qty = _number(record.get("qty"))
        payload = {"qty": qty} if qty is not None else None
        kind = "quantity"

    if payload is None:
        return None

    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return observed_at, local_date, source, kind, payload_json


def ingest_hae_payload(
    data_dir: Path,
    payload: object,
    *,
    received_at: datetime | None = None,
) -> dict[str, Any]:
    """Ingest the Health Auto Export JSON REST payload."""
    if not isinstance(payload, dict):
        raise HealthPayloadError("payload must be a JSON object")

    root = payload.get("data")
    if not isinstance(root, dict):
        raise HealthPayloadError("payload.data must be an object")
    groups = root.get("metrics")
    if not isinstance(groups, list):
        raise HealthPayloadError("payload.data.metrics must be an array")
    if len(groups) > MAX_METRIC_GROUPS:
        raise HealthPayloadError("too many metric groups")

    received = _iso_utc(received_at or _utc_now())
    accepted = 0
    ignored = 0
    total_seen = 0

    conn = _open_write(data_dir)
    try:
        with conn:
            for group in groups:
                if not isinstance(group, dict):
                    ignored += 1
                    continue
                metric = _canonical_metric(group.get("name"))
                records = group.get("data")
                if metric is None or not isinstance(records, list):
                    ignored += 1
                    continue
                if len(records) > MAX_RECORDS_PER_GROUP:
                    raise HealthPayloadError("too many records in one metric group")
                total_seen += len(records)
                if total_seen > MAX_TOTAL_RECORDS:
                    raise HealthPayloadError("too many records in one request")

                units = _safe_text(group.get("units"), default="", limit=64)
                for record in records:
                    normalized = _normalize_record(metric, units, record)
                    if normalized is None:
                        ignored += 1
                        continue
                    observed_at, local_date, source, kind, payload_json = normalized
                    conn.execute(
                        """
                        INSERT INTO metric_samples
                            (metric, observed_at, local_date, source, units, kind, payload_json, received_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(metric, observed_at, source) DO UPDATE SET
                            local_date=excluded.local_date,
                            units=excluded.units,
                            kind=excluded.kind,
                            payload_json=excluded.payload_json,
                            received_at=excluded.received_at
                        """,
                        (
                            metric,
                            observed_at,
                            local_date,
                            source,
                            units,
                            kind,
                            payload_json,
                            received,
                        ),
                    )
                    accepted += 1

            conn.execute(
                """
                INSERT INTO sync_state(key, value) VALUES('last_sync_at', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (received,),
            )
    finally:
        conn.close()

    return {"accepted": accepted, "ignored": ignored, "last_sync_at": received}
