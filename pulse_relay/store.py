"""Self-hosted Apple Health store fed by PulsHealth (Puls Sync Protocol v1).

The ingest side accepts protocol-v1 NDJSON batches and stores only an allow-listed
subset of metrics. Read helpers serve compact, freshness-aware snapshots to agents.

Rows written by the retired Health Auto Export (HAE) lane remain in older stores as
history: the readers still understand their ``range`` heart-rate and nightly ``sleep``
aggregate kinds, and prefer PulsHealth rows whenever both exist for a day.
"""
from __future__ import annotations

import json
import math
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final, TypedDict

DB_FILENAME: Final[str] = "health.sqlite3"
SOURCE_IDENTITY_VERSION: Final[str] = "2"

METRIC_ALIASES: Final[dict[str, str]] = {
    "active_energy_burned": "active_energy",
}

PULS_TYPE_TO_METRIC: Final[dict[str, str]] = {
    "HKQuantityTypeIdentifierHeartRate": "heart_rate",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "heart_rate_variability",
    "HKQuantityTypeIdentifierRestingHeartRate": "resting_heart_rate",
    "HKQuantityTypeIdentifierRespiratoryRate": "respiratory_rate",
    "HKQuantityTypeIdentifierAppleSleepingWristTemperature": "apple_sleeping_wrist_temperature",
    "HKQuantityTypeIdentifierOxygenSaturation": "blood_oxygen_saturation",
    "HKCategoryTypeIdentifierSleepAnalysis": "sleep_analysis",
    "HKQuantityTypeIdentifierStepCount": "step_count",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "walking_running_distance",
    "HKQuantityTypeIdentifierFlightsClimbed": "flights_climbed",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "active_energy",
    "HKQuantityTypeIdentifierAppleExerciseTime": "apple_exercise_time",
}

PULS_SLEEP_STAGES: Final[dict[int, str]] = {
    0: "in_bed",
    1: "asleep",
    2: "awake",
    3: "core",
    4: "deep",
    5: "rem",
}
PULS_COUNT_KEYS: Final[tuple[str, ...]] = (
    "sampleCount",
    "deletionCount",
    "routeCount",
    "seriesCount",
    "aggregateCount",
    "activitySummaryCount",
    "profileCount",
)

ALLOWED_METRICS: Final[frozenset[str]] = frozenset(
    {
        "heart_rate",
        "heart_rate_variability",
        "resting_heart_rate",
        "respiratory_rate",
        "apple_sleeping_wrist_temperature",
        "blood_oxygen_saturation",
        "sleep_analysis",
        "step_count",
        "walking_running_distance",
        "flights_climbed",
        "active_energy",
        "apple_exercise_time",
    }
)

LATEST_METRICS: Final[tuple[str, ...]] = (
    "heart_rate",
    "heart_rate_variability",
    "resting_heart_rate",
    "respiratory_rate",
    "apple_sleeping_wrist_temperature",
    "blood_oxygen_saturation",
)

COUNTER_METRICS: Final[tuple[str, ...]] = (
    "step_count",
    "walking_running_distance",
    "flights_climbed",
    "active_energy",
    "apple_exercise_time",
)

ACTIVITY_CANONICAL_UNITS: Final[dict[str, str]] = {
    "step_count": "count",
    "walking_running_distance": "km",
    "flights_climbed": "count",
    "active_energy": "kcal",
    "apple_exercise_time": "min",
}


class PulsPayloadError(ValueError):
    """Raised when a Puls Sync Protocol batch is malformed."""


class PulsProtocolVersionError(PulsPayloadError):
    """Raised when a Puls batch does not speak protocol version 1."""


class HealthNowResult(TypedDict):
    status: str
    last_sync_at: str | None
    freshness_seconds: int | None
    latest: dict[str, Any]
    today: dict[str, Any]
    sleep: dict[str, Any] | None


class HealthDetailResult(TypedDict):
    status: str
    metric: str
    hours: int
    last_sync_at: str | None
    samples: list[dict[str, Any]]
    summary: dict[str, Any] | None


class HealthDayResult(TypedDict):
    status: str
    date: str | None
    last_sync_at: str | None
    freshness_seconds: int | None
    latest: dict[str, Any]
    activity: dict[str, Any]
    sleep: dict[str, Any] | None
    heart_rate: dict[str, Any] | None


class HeartbeatResult(TypedDict):
    status: str
    bpm: float | int | None
    min_bpm: float | int | None
    max_bpm: float | int | None
    units: str | None
    source: str | None
    observed_at: str | None
    received_at: str | None
    age_seconds: int | None
    last_sync_at: str | None
    freshness_seconds: int | None


class PulsIngestResult(TypedDict):
    accepted: int
    ignored: int
    duplicates: int
    deleted: int
    aggregateSamples: int
    last_sync_at: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS metric_samples (
    metric TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    local_date TEXT NOT NULL,
    source TEXT NOT NULL,
    units TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (metric, observed_at, source)
);
CREATE INDEX IF NOT EXISTS metric_samples_metric_observed_idx
    ON metric_samples(metric, observed_at);
CREATE INDEX IF NOT EXISTS metric_samples_local_date_idx
    ON metric_samples(local_date, metric, observed_at);
CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS puls_batches (
    batch_id TEXT PRIMARY KEY,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS puls_sample_map (
    uuid TEXT PRIMARY KEY,
    metric TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL
);
"""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_utc_precise(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _safe_text(value: object, *, default: str, limit: int) -> str:
    if not isinstance(value, str):
        return default
    cleaned = " ".join(value.strip().split())
    if not cleaned:
        return default
    return cleaned[:limit]


def _canonical_source(value: object) -> str:
    """Collapse owner-renamable Apple device display names to stable source families."""
    cleaned = _safe_text(value, default="unknown", limit=160)
    folded = cleaned.casefold()
    if "apple watch" in folded:
        return "Apple Watch"
    if "iphone" in folded:
        return "iPhone"
    return cleaned


def _number(value: object) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(float(value)):
        return None
    return value


def _canonical_metric(name: object) -> str | None:
    if not isinstance(name, str):
        return None
    normalized = name.strip().lower()
    normalized = METRIC_ALIASES.get(normalized, normalized)
    if normalized not in ALLOWED_METRICS:
        return None
    return normalized


def _migrate_source_identity(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT value FROM sync_state WHERE key='source_identity_version'").fetchone()
    if row is not None and str(row["value"]) == SOURCE_IDENTITY_VERSION:
        return

    rows = conn.execute(
        "SELECT * FROM metric_samples ORDER BY received_at ASC, rowid ASC"
    ).fetchall()
    with conn:
        conn.execute("DROP TABLE IF EXISTS metric_samples_source_v2")
        conn.execute(
            """
            CREATE TABLE metric_samples_source_v2 (
                metric TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                local_date TEXT NOT NULL,
                source TEXT NOT NULL,
                units TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                received_at TEXT NOT NULL,
                PRIMARY KEY (metric, observed_at, source)
            )
            """
        )
        for item in rows:
            conn.execute(
                """
                INSERT INTO metric_samples_source_v2
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
                    item["metric"],
                    item["observed_at"],
                    item["local_date"],
                    _canonical_source(item["source"]),
                    item["units"],
                    item["kind"],
                    item["payload_json"],
                    item["received_at"],
                ),
            )
        conn.execute("DROP TABLE metric_samples")
        conn.execute("ALTER TABLE metric_samples_source_v2 RENAME TO metric_samples")
        conn.execute(
            "CREATE INDEX metric_samples_metric_observed_idx ON metric_samples(metric, observed_at)"
        )
        conn.execute(
            "CREATE INDEX metric_samples_local_date_idx ON metric_samples(local_date, metric, observed_at)"
        )
        conn.execute(
            """
            INSERT INTO sync_state(key, value) VALUES('source_identity_version', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (SOURCE_IDENTITY_VERSION,),
        )


def _open_write(data_dir: Path) -> sqlite3.Connection:
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(data_dir / DB_FILENAME, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_SCHEMA)
    _migrate_source_identity(conn)
    return conn


def initialize_store(data_dir: Path) -> None:
    """Create/migrate the store before serving reads."""
    conn = _open_write(data_dir)
    conn.close()


def _open_read(data_dir: Path) -> sqlite3.Connection | None:
    path = data_dir / DB_FILENAME
    if not path.is_file():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _valid_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value)).lower()
    except (ValueError, AttributeError):
        return None


def _parse_epoch_ms(value: object, context: object = None) -> tuple[str, str]:
    parsed = _number(value)
    if parsed is None or parsed == 0:
        raise PulsPayloadError("timestamp must be a finite epoch-ms number")
    try:
        instant = datetime.fromtimestamp(float(parsed) / 1000.0, timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise PulsPayloadError("timestamp is out of range") from None
    offset = 0.0
    if isinstance(context, dict):
        candidate = _number(context.get("utcOffsetSeconds"))
        if candidate is not None and -86400 <= float(candidate) <= 86400:
            offset = float(candidate)
    local = datetime.fromtimestamp(float(parsed) / 1000.0 + offset, timezone.utc)
    return _iso_utc_precise(instant), local.date().isoformat()


def _puls_source(sample: dict[str, Any]) -> str:
    device = sample.get("device")
    if isinstance(device, str):
        folded = device.casefold()
        if "apple watch" in folded or "iphone" in folded:
            return _canonical_source(device)
    return _canonical_source(sample.get("sourceName"))


def _parse_puls_ndjson(text: str) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    objects: list[dict[str, Any]] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PulsPayloadError(f"line {number} is not valid JSON") from exc
        if not isinstance(item, dict):
            raise PulsPayloadError(f"line {number} must be an object")
        objects.append(item)
    if not objects:
        raise PulsPayloadError("empty body")

    header = objects[0]
    version = header.get("schemaVersion")
    if version not in (None, 1):
        raise PulsProtocolVersionError("unsupported protocol version")
    if _valid_uuid(header.get("batchID")) is None or not isinstance(header.get("type"), str) or not header["type"]:
        raise PulsPayloadError("invalid batch header")

    counts: dict[str, int] = {}
    for key in PULS_COUNT_KEYS:
        value = header.get(key, 0)
        if value is None:
            value = 0
        if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 100_000:
            raise PulsPayloadError(f"invalid {key}")
        counts[key] = value
    if counts["profileCount"] > 1 or sum(counts.values()) > 200_000:
        raise PulsPayloadError("too many batch lines")
    if len(objects) != 1 + sum(counts.values()):
        raise PulsPayloadError("batch line counts do not match header")

    cursor = 1
    out: dict[str, list[dict[str, Any]]] = {
        "samples": [], "deletions": [], "routes": [], "series": [],
        "aggregates": [], "activitySummaries": [], "profiles": [],
    }
    sample_count = counts["sampleCount"]
    out["samples"] = objects[cursor:cursor + sample_count]
    cursor += sample_count

    for count_key, out_key, wrapper in (
        ("deletionCount", "deletions", "deleted"),
        ("routeCount", "routes", "route"),
        ("seriesCount", "series", "series"),
        ("aggregateCount", "aggregates", "aggregate"),
        ("activitySummaryCount", "activitySummaries", "activitySummary"),
        ("profileCount", "profiles", "profile"),
    ):
        count = counts[count_key]
        for item in objects[cursor:cursor + count]:
            inner = item.get(wrapper)
            if not isinstance(inner, dict):
                raise PulsPayloadError(f"missing {wrapper} wrapper")
            out[out_key].append(inner)
        cursor += count
    return header, out


def _normalize_puls_sample(metric: str, sample: dict[str, Any]) -> tuple[str, str, str, str, str, str, str] | None:
    sample_uuid = _valid_uuid(sample.get("uuid"))
    if sample_uuid is None:
        raise PulsPayloadError("sample uuid is invalid")
    observed_at, local_date = _parse_epoch_ms(sample.get("start"), sample.get("startContext"))
    source = _puls_source(sample)
    units = _safe_text(sample.get("unit"), default="", limit=64)

    if metric == "sleep_analysis":
        if sample.get("kind") != "category":
            return None
        category = sample.get("category")
        if isinstance(category, bool) or not isinstance(category, int):
            raise PulsPayloadError("sleep category is invalid")
        stage = PULS_SLEEP_STAGES.get(category, f"category_{category}")
        end_raw = sample.get("end") or sample.get("start")
        end_at, _ = _parse_epoch_ms(end_raw, sample.get("endContext") or sample.get("startContext"))
        payload = {"stage": stage, "start": observed_at, "end": end_at}
        kind = "sleep_segment"
    else:
        if sample.get("kind") != "quantity":
            return None
        value = _number(sample.get("value"))
        if value is None:
            return None
        payload = {"qty": value}
        kind = "quantity"

    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sample_uuid, observed_at, local_date, source, units, kind, payload_json


def ingest_puls_ndjson(
    data_dir: Path,
    text: str,
    *,
    received_at: datetime | None = None,
) -> PulsIngestResult:
    """Ingest one Puls Sync Protocol v1 NDJSON batch into the shared metric store."""
    header, lines = _parse_puls_ndjson(text)
    batch_id = _valid_uuid(header.get("batchID"))
    assert batch_id is not None
    received = _iso_utc(received_at or _utc_now())
    accepted = 0
    ignored = 0
    duplicates = 0
    deleted = 0
    aggregate_samples = 0

    conn = _open_write(data_dir)
    try:
        with conn:
            replay = conn.execute("SELECT 1 FROM puls_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if replay is not None:
                return {
                    "accepted": 0,
                    "ignored": 0,
                    "duplicates": int(header.get("sampleCount") or 0),
                    "deleted": 0,
                    "aggregateSamples": 0,
                    "last_sync_at": _state_value(conn, "last_sync_at") or received,
                }
            conn.execute("INSERT INTO puls_batches(batch_id, received_at) VALUES (?, ?)", (batch_id, received))

            for sample in lines["samples"]:
                metric = PULS_TYPE_TO_METRIC.get(sample.get("type"))
                if metric is None or metric not in ALLOWED_METRICS:
                    ignored += 1
                    continue
                normalized = _normalize_puls_sample(metric, sample)
                if normalized is None:
                    ignored += 1
                    continue
                sample_uuid, observed_at, local_date, source, units, kind, payload_json = normalized
                if conn.execute("SELECT 1 FROM puls_sample_map WHERE uuid=?", (sample_uuid,)).fetchone() is not None:
                    duplicates += 1
                    continue
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
                    (metric, observed_at, local_date, source, units, kind, payload_json, received),
                )
                conn.execute(
                    "INSERT INTO puls_sample_map(uuid, metric, observed_at, source) VALUES (?, ?, ?, ?)",
                    (sample_uuid, metric, observed_at, source),
                )
                accepted += 1

            for item in lines["deletions"]:
                sample_uuid = _valid_uuid(item.get("uuid"))
                if sample_uuid is None or not isinstance(item.get("type"), str):
                    raise PulsPayloadError("deletion is invalid")
                mapped = conn.execute(
                    "SELECT metric, observed_at, source FROM puls_sample_map WHERE uuid=?", (sample_uuid,)
                ).fetchone()
                if mapped is None:
                    continue
                deleted += conn.execute(
                    "DELETE FROM metric_samples WHERE metric=? AND observed_at=? AND source=?",
                    (mapped["metric"], mapped["observed_at"], mapped["source"]),
                ).rowcount
                conn.execute("DELETE FROM puls_sample_map WHERE uuid=?", (sample_uuid,))

            for aggregate in lines["aggregates"]:
                metric = PULS_TYPE_TO_METRIC.get(aggregate.get("type"))
                if metric not in COUNTER_METRICS or aggregate.get("func") != "sum":
                    ignored += 1
                    continue
                value = _number(aggregate.get("value"))
                interval_value = aggregate.get("intervalValue")
                interval_unit = aggregate.get("intervalUnit")
                device_filter = aggregate.get("deviceFilter")
                if value is None:
                    ignored += 1
                    continue
                if isinstance(interval_value, bool) or not isinstance(interval_value, int) or interval_value < 1:
                    raise PulsPayloadError("aggregate interval is invalid")
                if interval_unit not in {"minute", "hour", "day", "week", "month"}:
                    raise PulsPayloadError("aggregate interval unit is invalid")
                if device_filter not in {"all", "watch", "iphone"}:
                    raise PulsPayloadError("aggregate device filter is invalid")
                observed_at, local_date = _parse_epoch_ms(
                    aggregate.get("bucketStart"), aggregate.get("bucketStartContext")
                )
                bucket_end, _ = _parse_epoch_ms(
                    aggregate.get("bucketEnd"), aggregate.get("bucketEndContext") or aggregate.get("bucketStartContext")
                )
                if _datetime_from_iso(bucket_end) <= _datetime_from_iso(observed_at):
                    raise PulsPayloadError("aggregate bucket end is invalid")
                source = f"PulsHealth aggregate:{device_filter}:{interval_value}:{interval_unit}"
                units = _safe_text(aggregate.get("unit"), default="", limit=64)
                payload = {
                    "qty": value,
                    "func": "sum",
                    "interval_value": interval_value,
                    "interval_unit": interval_unit,
                    "device_filter": device_filter,
                    "bucket_end": bucket_end,
                }
                payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                conn.execute(
                    """
                    INSERT INTO metric_samples
                        (metric, observed_at, local_date, source, units, kind, payload_json, received_at)
                    VALUES (?, ?, ?, ?, ?, 'aggregate', ?, ?)
                    ON CONFLICT(metric, observed_at, source) DO UPDATE SET
                        local_date=excluded.local_date,
                        units=excluded.units,
                        kind=excluded.kind,
                        payload_json=excluded.payload_json,
                        received_at=excluded.received_at
                    """,
                    (metric, observed_at, local_date, source, units, payload_json, received),
                )
                aggregate_samples += 1

            # Routes, workout series, activity summaries, and profile lines are valid v1
            # input but outside this compact AI-context store. Accept and discard them.
            ignored += len(lines["routes"]) + len(lines["series"]) + len(lines["activitySummaries"]) + len(lines["profiles"])
            conn.execute(
                """
                INSERT INTO sync_state(key, value) VALUES('last_sync_at', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (received,),
            )
    finally:
        conn.close()

    return {
        "accepted": accepted,
        "ignored": ignored,
        "duplicates": duplicates,
        "deleted": deleted,
        "aggregateSamples": aggregate_samples,
        "last_sync_at": received,
    }


def _state_value(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM sync_state WHERE key=?", (key,)).fetchone()
    return str(row["value"]) if row is not None else None


def _age_seconds(timestamp: str | None, *, now: datetime) -> int | None:
    if timestamp is None:
        return None
    try:
        text = timestamp[:-1] + "+00:00" if timestamp.endswith("Z") else timestamp
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return max(0, int((now - parsed).total_seconds()))


def _row_payload(row: sqlite3.Row, *, now: datetime) -> dict[str, Any]:
    value = json.loads(row["payload_json"])
    return {
        "value": value,
        "units": row["units"],
        "source": row["source"],
        "observed_at": row["observed_at"],
        "received_at": row["received_at"],
        "age_seconds": _age_seconds(row["observed_at"], now=now),
    }


def _normalized_activity_value(metric: str, value: float, units: object) -> tuple[float, str] | None:
    """Convert activity counters to stable read-side units without rewriting stored history."""
    unit = _safe_text(units, default="", limit=64).casefold().replace(" ", "")
    canonical = ACTIVITY_CANONICAL_UNITS.get(metric)
    if canonical is None:
        return None

    if metric in {"step_count", "flights_climbed"}:
        if unit in {"", "count", "counts"}:
            return value, canonical
        return None

    if metric == "walking_running_distance":
        if unit in {"km", "kilometer", "kilometers", "kilometre", "kilometres"}:
            return value, canonical
        if unit in {"m", "meter", "meters", "metre", "metres"}:
            return value / 1000.0, canonical
        if unit in {"mi", "mile", "miles"}:
            return value * 1.609344, canonical
        return None

    if metric == "active_energy":
        if unit in {"kcal", "cal", "kilocalorie", "kilocalories"}:
            return value, canonical
        if unit in {"kj", "kilojoule", "kilojoules"}:
            return value / 4.184, canonical
        if unit in {"j", "joule", "joules"}:
            return value / 4184.0, canonical
        return None

    if metric == "apple_exercise_time":
        if unit in {"min", "mins", "minute", "minutes"}:
            return value, canonical
        if unit in {"h", "hr", "hrs", "hour", "hours"}:
            return value * 60.0, canonical
        if unit in {"s", "sec", "secs", "second", "seconds"}:
            return value / 60.0, canonical
        return None

    return None


def _puls_rows_for_day(
    conn: sqlite3.Connection,
    metric: str,
    target_date: str,
) -> list[sqlite3.Row]:
    """Return UUID-backed PulsHealth samples for one metric/day."""
    return conn.execute(
        """
        SELECT s.*
        FROM metric_samples AS s
        JOIN puls_sample_map AS p
          ON p.metric=s.metric
         AND p.observed_at=s.observed_at
         AND p.source=s.source
        WHERE s.metric=? AND s.local_date=?
        ORDER BY s.observed_at ASC
        """,
        (metric, target_date),
    ).fetchall()


def _puls_keys_for_day(
    conn: sqlite3.Connection,
    metric: str,
    target_date: str,
) -> set[tuple[str, str]]:
    return {
        (str(row["observed_at"]), str(row["source"]))
        for row in _puls_rows_for_day(conn, metric, target_date)
    }


def _datetime_from_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _sleep_for_date(
    conn: sqlite3.Connection,
    target_date: str,
    *,
    now: datetime,
) -> dict[str, Any] | None:
    """Return the best PulsHealth sleep session, else a legacy HAE nightly aggregate."""
    session = _puls_sleep_session(conn, target_date, now=now)
    if session is not None:
        return session
    aggregate_row = conn.execute(
        """
        SELECT * FROM metric_samples
        WHERE metric='sleep_analysis' AND local_date=? AND kind='sleep'
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        (target_date,),
    ).fetchone()
    return _row_payload(aggregate_row, now=now) if aggregate_row is not None else None


def _puls_sleep_session(
    conn: sqlite3.Connection,
    target_date: str,
    *,
    now: datetime,
) -> dict[str, Any] | None:
    """Group PulsHealth sleep segments into the best overnight session touching ``target_date``."""
    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    previous = (target - timedelta(days=1)).isoformat()
    rows = conn.execute(
        """
        SELECT * FROM metric_samples
        WHERE metric='sleep_analysis'
          AND kind='sleep_segment'
          AND local_date IN (?, ?)
        ORDER BY observed_at ASC
        """,
        (previous, target_date),
    ).fetchall()
    if not rows:
        return None

    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        payload = json.loads(row["payload_json"])
        stage = payload.get("stage")
        start = _datetime_from_iso(payload.get("start"))
        end = _datetime_from_iso(payload.get("end"))
        if stage not in {"in_bed", "asleep", "awake", "core", "deep", "rem"}:
            continue
        if start is None or end is None or end <= start or end - start > timedelta(hours=24):
            continue
        by_source.setdefault(str(row["source"]), []).append(
            {
                "row": row,
                "stage": str(stage),
                "start": start,
                "end": end,
                "local_date": str(row["local_date"]),
            }
        )

    candidates: list[tuple[float, datetime, dict[str, Any]]] = []
    for source, source_segments in by_source.items():
        source_segments.sort(key=lambda item: item["start"])
        clusters: list[list[dict[str, Any]]] = []
        current_cluster: list[dict[str, Any]] = []
        current_end: datetime | None = None
        for segment in source_segments:
            if (
                current_cluster
                and current_end is not None
                and segment["start"] > current_end + timedelta(hours=2)
            ):
                clusters.append(current_cluster)
                current_cluster = []
                current_end = None
            current_cluster.append(segment)
            if current_end is None or segment["end"] > current_end:
                current_end = segment["end"]
        if current_cluster:
            clusters.append(current_cluster)

        for cluster in clusters:
            if not any(item["local_date"] == target_date for item in cluster):
                continue

            has_detailed = any(item["stage"] in {"core", "deep", "rem"} for item in cluster)
            sleep_stage_names = {"core", "deep", "rem"} if has_detailed else {"asleep"}
            sleep_segments = [item for item in cluster if item["stage"] in sleep_stage_names]
            if not sleep_segments:
                continue

            hours: dict[str, float] = {
                "core": 0.0,
                "rem": 0.0,
                "deep": 0.0,
                "awake": 0.0,
                "in_bed": 0.0,
            }
            generic_asleep = 0.0
            for item in cluster:
                duration_hours = (item["end"] - item["start"]).total_seconds() / 3600.0
                stage = item["stage"]
                if stage in hours:
                    hours[stage] += duration_hours
                elif stage == "asleep" and not has_detailed:
                    generic_asleep += duration_hours

            sleep_start = min(item["start"] for item in sleep_segments)
            sleep_end = max(item["end"] for item in sleep_segments)
            in_bed_segments = [item for item in cluster if item["stage"] == "in_bed"]
            value: dict[str, Any] = {
                "core": round(hours["core"], 6),
                "rem": round(hours["rem"], 6),
                "deep": round(hours["deep"], 6),
                "awake": round(hours["awake"], 6),
                "in_bed": round(hours["in_bed"], 6),
                "sleep_start": _iso_utc_precise(sleep_start),
                "sleep_end": _iso_utc_precise(sleep_end),
            }
            if not has_detailed:
                value["asleep"] = round(generic_asleep, 6)
            if in_bed_segments:
                value["in_bed_start"] = _iso_utc_precise(min(item["start"] for item in in_bed_segments))
                value["in_bed_end"] = _iso_utc_precise(max(item["end"] for item in in_bed_segments))

            total_sleep = sum(
                (item["end"] - item["start"]).total_seconds() / 3600.0
                for item in sleep_segments
            )
            latest_received = max(str(item["row"]["received_at"]) for item in cluster)
            result = {
                "value": value,
                "units": "hr",
                "source": source,
                "observed_at": _iso_utc_precise(sleep_end),
                "received_at": latest_received,
                "age_seconds": _age_seconds(_iso_utc_precise(sleep_end), now=now),
            }
            candidates.append((total_sleep, sleep_end, result))

    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return candidates[0][2]


def _empty_now() -> HealthNowResult:
    return {
        "status": "empty",
        "last_sync_at": None,
        "freshness_seconds": None,
        "latest": {},
        "today": {},
        "sleep": None,
    }


def health_now(data_dir: Path, *, now: datetime | None = None) -> HealthNowResult:
    """Return a compact snapshot suitable for an AI tool call."""
    current = (now or _utc_now()).astimezone(timezone.utc)
    conn = _open_read(data_dir)
    if conn is None:
        return _empty_now()

    try:
        last_sync = _state_value(conn, "last_sync_at")
        latest: dict[str, Any] = {}
        for metric in LATEST_METRICS:
            row = conn.execute(
                """
                SELECT * FROM metric_samples
                WHERE metric=?
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                (metric,),
            ).fetchone()
            if row is not None:
                latest[metric] = _row_payload(row, now=current)

        latest_any = conn.execute(
            "SELECT local_date FROM metric_samples ORDER BY observed_at DESC LIMIT 1"
        ).fetchone()
        today: dict[str, Any] = {}
        if latest_any is not None:
            target_date = str(latest_any["local_date"])
            today = _activity_for_date(conn, target_date)

        sleep = _sleep_for_date(conn, target_date, now=current) if latest_any is not None else None

        has_data = bool(latest or today or sleep)
        return {
            "status": "ok" if has_data else "empty",
            "last_sync_at": last_sync,
            "freshness_seconds": _age_seconds(last_sync, now=current),
            "latest": latest,
            "today": today,
            "sleep": sleep,
        }
    finally:
        conn.close()


def _validated_local_date(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("date must be YYYY-MM-DD")
    text = value.strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        raise ValueError("date must be YYYY-MM-DD") from None
    normalized = parsed.date().isoformat()
    if normalized != text:
        raise ValueError("date must be YYYY-MM-DD")
    return normalized


def _highest_source_rows(
    rows: list[tuple[sqlite3.Row, float]],
) -> list[tuple[sqlite3.Row, float]]:
    if not rows:
        return []
    by_source: dict[str, list[tuple[sqlite3.Row, float]]] = {}
    for item in rows:
        by_source.setdefault(str(item[0]["source"]), []).append(item)
    return max(
        by_source.values(),
        key=lambda items: sum(value for _, value in items),
    )


def _hourly_bucket_end(row: sqlite3.Row, payload: dict[str, Any]) -> datetime | None:
    explicit = _datetime_from_iso(payload.get("bucket_end"))
    if explicit is not None:
        return explicit
    start = _datetime_from_iso(row["observed_at"])
    if start is None:
        return None
    interval_value = payload.get("interval_value")
    if isinstance(interval_value, bool) or not isinstance(interval_value, int) or interval_value < 1:
        return None
    if payload.get("interval_unit") != "hour":
        return None
    return start + timedelta(hours=interval_value)


def _activity_for_date(conn: sqlite3.Connection, target_date: str) -> dict[str, Any]:
    activity: dict[str, Any] = {}
    for metric in COUNTER_METRICS:
        rows = conn.execute(
            """
            SELECT * FROM metric_samples
            WHERE metric=? AND local_date=?
            ORDER BY observed_at ASC
            """,
            (metric, target_date),
        ).fetchall()
        if not rows:
            continue

        daily_all: list[tuple[sqlite3.Row, float]] = []
        hourly_all: list[tuple[sqlite3.Row, float, datetime]] = []
        raw_values: list[tuple[sqlite3.Row, float]] = []
        puls_keys = _puls_keys_for_day(conn, metric, target_date)

        for row in rows:
            payload = json.loads(row["payload_json"])
            qty = _number(payload.get("qty"))
            if qty is None:
                continue
            normalized = _normalized_activity_value(metric, float(qty), row["units"])
            if normalized is None:
                continue
            normalized_value, _ = normalized

            if row["kind"] == "aggregate" and payload.get("func") == "sum":
                device_filter = payload.get("device_filter")
                interval_value = payload.get("interval_value")
                interval_unit = payload.get("interval_unit")
                if device_filter == "all" and interval_value == 1 and interval_unit == "day":
                    daily_all.append((row, normalized_value))
                elif device_filter == "all" and interval_value == 1 and interval_unit == "hour":
                    bucket_end = _hourly_bucket_end(row, payload)
                    if bucket_end is not None:
                        hourly_all.append((row, normalized_value, bucket_end))
                continue

            if row["kind"] == "aggregate":
                continue

            key = (str(row["observed_at"]), str(row["source"]))
            if puls_keys and key not in puls_keys:
                continue
            raw_values.append((row, normalized_value))

        chosen: list[tuple[sqlite3.Row, float]]
        if daily_all:
            chosen = daily_all
        elif hourly_all:
            latest_end = max(bucket_end for _, _, bucket_end in hourly_all)
            aggregate_rows = [(row, value) for row, value, _ in hourly_all]
            tail_candidates = []
            for item in raw_values:
                observed = _datetime_from_iso(item[0]["observed_at"])
                if observed is not None and observed >= latest_end:
                    tail_candidates.append(item)
            chosen = aggregate_rows + _highest_source_rows(tail_candidates)
        else:
            chosen = _highest_source_rows(raw_values)

        if not chosen:
            continue

        values = [value for _, value in chosen]
        last_row = max(chosen, key=lambda item: str(item[0]["observed_at"]))[0]
        total = sum(values)
        total_out: float | int = int(total) if all(value.is_integer() for value in values) else round(total, 6)
        activity[metric] = {
            "qty": total_out,
            "units": ACTIVITY_CANONICAL_UNITS[metric],
            "local_date": target_date,
            "buckets": len(values),
            "last_observed_at": last_row["observed_at"],
        }
    return activity


def _heart_rate_summary_for_date(conn: sqlite3.Connection, target_date: str) -> dict[str, Any] | None:
    # Prefer UUID-backed PulsHealth raw samples when a day contains both the
    # legacy HAE export and the new PulsHealth backfill. HAE can round/sample
    # timestamps differently, so the primary key alone cannot collapse every
    # cross-transport duplicate.
    rows = _puls_rows_for_day(conn, "heart_rate", target_date)
    if not rows:
        rows = conn.execute(
            """
            SELECT * FROM metric_samples
            WHERE metric='heart_rate' AND local_date=?
            ORDER BY observed_at ASC
            """,
            (target_date,),
        ).fetchall()
    if not rows:
        return None

    avgs: list[float] = []
    mins: list[float] = []
    maxs: list[float] = []
    for row in rows:
        payload = json.loads(row["payload_json"])
        avg = _number(payload.get("avg"))
        if avg is None:
            avg = _number(payload.get("qty"))
        low = _number(payload.get("min"))
        high = _number(payload.get("max"))
        if avg is not None:
            avgs.append(float(avg))
        if low is not None:
            mins.append(float(low))
        if high is not None:
            maxs.append(float(high))

    last_row = rows[-1]
    last_payload = json.loads(last_row["payload_json"])
    latest = _number(last_payload.get("avg"))
    if latest is None:
        latest = _number(last_payload.get("qty"))
    result: dict[str, Any] = {
        "count": len(rows),
        "units": last_row["units"],
        "latest": latest,
        "last_observed_at": last_row["observed_at"],
    }
    if avgs:
        result["avg"] = sum(avgs) / len(avgs)
    if mins:
        result["min"] = min(mins)
    elif avgs:
        result["min"] = min(avgs)
    if maxs:
        result["max"] = max(maxs)
    elif avgs:
        result["max"] = max(avgs)
    return result


def health_day(
    data_dir: Path,
    date: str | None = None,
    *,
    now: datetime | None = None,
) -> HealthDayResult:
    """Return one local calendar day's compact health context.

    When date is omitted, use the latest local_date actually present in the
    store rather than assuming the caller's current date or timezone.
    """
    current = (now or _utc_now()).astimezone(timezone.utc)
    requested_date = _validated_local_date(date) if date is not None else None
    conn = _open_read(data_dir)
    if conn is None:
        return {
            "status": "empty",
            "date": requested_date,
            "last_sync_at": None,
            "freshness_seconds": None,
            "latest": {},
            "activity": {},
            "sleep": None,
            "heart_rate": None,
        }

    try:
        last_sync = _state_value(conn, "last_sync_at")
        target_date = requested_date
        if target_date is None:
            latest_any = conn.execute(
                "SELECT local_date FROM metric_samples ORDER BY observed_at DESC LIMIT 1"
            ).fetchone()
            target_date = str(latest_any["local_date"]) if latest_any is not None else None

        if target_date is None:
            return {
                "status": "empty",
                "date": None,
                "last_sync_at": last_sync,
                "freshness_seconds": _age_seconds(last_sync, now=current),
                "latest": {},
                "activity": {},
                "sleep": None,
                "heart_rate": None,
            }

        latest: dict[str, Any] = {}
        for metric in LATEST_METRICS:
            row = conn.execute(
                """
                SELECT * FROM metric_samples
                WHERE metric=? AND local_date=?
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                (metric, target_date),
            ).fetchone()
            if row is not None:
                latest[metric] = _row_payload(row, now=current)

        activity = _activity_for_date(conn, target_date)
        sleep = _sleep_for_date(conn, target_date, now=current)
        heart_rate = _heart_rate_summary_for_date(conn, target_date)
        has_data = bool(latest or activity or sleep or heart_rate)
        return {
            "status": "ok" if has_data else "empty",
            "date": target_date,
            "last_sync_at": last_sync,
            "freshness_seconds": _age_seconds(last_sync, now=current),
            "latest": latest,
            "activity": activity,
            "sleep": sleep,
            "heart_rate": heart_rate,
        }
    finally:
        conn.close()


def heartbeat(data_dir: Path, *, now: datetime | None = None) -> HeartbeatResult:
    """Return the latest available heart-rate sample with freshness metadata."""
    current = (now or _utc_now()).astimezone(timezone.utc)
    conn = _open_read(data_dir)
    empty: HeartbeatResult = {
        "status": "empty",
        "bpm": None,
        "min_bpm": None,
        "max_bpm": None,
        "units": None,
        "source": None,
        "observed_at": None,
        "received_at": None,
        "age_seconds": None,
        "last_sync_at": None,
        "freshness_seconds": None,
    }
    if conn is None:
        return empty

    try:
        last_sync = _state_value(conn, "last_sync_at")
        row = conn.execute(
            """
            SELECT * FROM metric_samples
            WHERE metric='heart_rate'
            ORDER BY observed_at DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            empty["last_sync_at"] = last_sync
            empty["freshness_seconds"] = _age_seconds(last_sync, now=current)
            return empty
        payload = json.loads(row["payload_json"])
        bpm = _number(payload.get("avg"))
        if bpm is None:
            bpm = _number(payload.get("qty"))
        return {
            "status": "ok" if bpm is not None else "empty",
            "bpm": bpm,
            "min_bpm": _number(payload.get("min")),
            "max_bpm": _number(payload.get("max")),
            "units": str(row["units"]),
            "source": str(row["source"]),
            "observed_at": str(row["observed_at"]),
            "received_at": str(row["received_at"]),
            "age_seconds": _age_seconds(str(row["observed_at"]), now=current),
            "last_sync_at": last_sync,
            "freshness_seconds": _age_seconds(last_sync, now=current),
        }
    finally:
        conn.close()


def health_detail(
    data_dir: Path,
    metric: str,
    *,
    hours: int = 2,
    limit: int = 120,
    now: datetime | None = None,
) -> HealthDetailResult:
    canonical = _canonical_metric(metric)
    if canonical is None:
        raise ValueError("metric is not allow-listed")
    if not 1 <= hours <= 48:
        raise ValueError("hours must be between 1 and 48")
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")

    current = (now or _utc_now()).astimezone(timezone.utc)
    conn = _open_read(data_dir)
    if conn is None:
        return {
            "status": "empty",
            "metric": canonical,
            "hours": hours,
            "last_sync_at": None,
            "samples": [],
            "summary": None,
        }

    try:
        cutoff = _iso_utc(current - timedelta(hours=hours))
        rows = conn.execute(
            """
            SELECT * FROM metric_samples
            WHERE metric=? AND observed_at>=?
            ORDER BY observed_at DESC
            LIMIT ?
            """,
            (canonical, cutoff, limit),
        ).fetchall()
        rows = list(reversed(rows))
        samples = [_row_payload(row, now=current) for row in rows]

        numeric: list[float] = []
        for sample in samples:
            value = sample["value"]
            candidate = value.get("avg") if isinstance(value, dict) else None
            if candidate is None and isinstance(value, dict):
                candidate = value.get("qty")
            parsed = _number(candidate)
            if parsed is not None:
                numeric.append(float(parsed))

        summary: dict[str, Any] | None = None
        if numeric:
            summary = {
                "count": len(numeric),
                "min": min(numeric),
                "max": max(numeric),
                "avg": sum(numeric) / len(numeric),
            }

        return {
            "status": "ok" if samples else "empty",
            "metric": canonical,
            "hours": hours,
            "last_sync_at": _state_value(conn, "last_sync_at"),
            "samples": samples,
            "summary": summary,
        }
    finally:
        conn.close()


__all__ = [
    "ALLOWED_METRICS",
    "COUNTER_METRICS",
    "initialize_store",
    "DB_FILENAME",
    "HealthDayResult",
    "HealthDetailResult",
    "HealthNowResult",
    "HeartbeatResult",
    "PulsPayloadError",
    "PulsProtocolVersionError",
    "PulsIngestResult",
    "health_day",
    "health_detail",
    "health_now",
    "heartbeat",
    "ingest_puls_ndjson",
]
