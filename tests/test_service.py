from __future__ import annotations

import gzip
import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pulse_relay.server import ServiceSettings, build_http_server
from legacy_hae import HealthPayloadError, ingest_hae_payload
from pulse_relay.store import (
    health_day,
    health_detail,
    health_now,
    heartbeat,
    ingest_puls_ndjson,
    initialize_store,
)


def payload(step_first: int = 100):
    return {
        "data": {
            "metrics": [
                {
                    "name": "heart_rate",
                    "units": "count/min",
                    "data": [
                        {
                            "date": "2026-09-29 10:00:00 +0000",
                            "source": "Apple Watch",
                            "Min": 61,
                            "Avg": 72,
                            "Max": 94,
                            "metadata": {"should": "not persist"},
                        },
                        {
                            "date": "2026-09-29 11:00:00 +0000",
                            "source": "Apple Watch",
                            "Min": 64,
                            "Avg": 79,
                            "Max": 101,
                        },
                    ],
                },
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {
                            "date": "2026-09-29 10:00:00 +0000",
                            "source": "Apple Watch",
                            "qty": step_first,
                        },
                        {
                            "date": "2026-09-29 11:00:00 +0000",
                            "source": "Apple Watch",
                            "qty": 200,
                        },
                    ],
                },
                {
                    "name": "active_energy",
                    "units": "kcal",
                    "data": [
                        {
                            "date": "2026-09-29 10:00:00 +0000",
                            "source": "Apple Watch",
                            "qty": 22.5,
                        },
                        {
                            "date": "2026-09-29 11:00:00 +0000",
                            "source": "Apple Watch",
                            "qty": 30.25,
                        },
                    ],
                },
                {
                    "name": "sleep_analysis",
                    "units": "hr",
                    "data": [
                        {
                            "date": "2026-09-29 08:00:00 +0000",
                            "source": "Apple Watch",
                            "sleepStart": "2026-09-29 00:30:00 +0000",
                            "sleepEnd": "2026-09-29 08:00:00 +0000",
                            "core": 4.0,
                            "rem": 1.5,
                            "deep": 1.0,
                            "awake": 0.25,
                            "inBed": 7.5,
                        }
                    ],
                },
                {
                    "name": "dietary_water",
                    "units": "mL",
                    "data": [
                        {
                            "date": "2026-09-29 10:00:00 +0000",
                            "source": "Other",
                            "qty": 999,
                        }
                    ],
                },
            ]
        }
    }


def puls_batch(batch_id: str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa") -> bytes:
    start_ms = 1791327600000  # 2026-10-06 23:00:00 UTC
    context = {
        "timeZoneID": "UTC",
        "utcOffsetSeconds": 0,
        "source": "device_current",
        "confidence": "inferred",
        "tzdbVersion": "",
    }
    lines = [
        {
            "schemaVersion": 1,
            "clientVersion": "test",
            "batchID": batch_id,
            "deviceID": "test-device",
            "type": "HKQuantityTypeIdentifierHeartRate",
            "reason": "manual",
            "exportedAt": start_ms + 30_000,
            "sampleCount": 7,
            "deletionCount": 0,
            "routeCount": 0,
            "seriesCount": 0,
            "aggregateCount": 1,
            "activitySummaryCount": 0,
            "profileCount": 0,
        },
        {
            "uuid": "11111111-1111-4111-8111-111111111111",
            "type": "HKQuantityTypeIdentifierHeartRate",
            "kind": "quantity",
            "start": start_ms,
            "end": start_ms,
            "startContext": context,
            "endContext": context,
            "value": 81,
            "unit": "count/min",
            "sourceName": "Apple Watch",
            "device": "Apple Watch",
        },
        {
            "uuid": "22222222-2222-4222-8222-222222222222",
            "type": "HKQuantityTypeIdentifierStepCount",
            "kind": "quantity",
            "start": start_ms,
            "end": start_ms,
            "startContext": context,
            "endContext": context,
            "value": 100,
            "unit": "count",
            "sourceName": "iPhone",
            "device": "iPhone",
        },
        {
            "uuid": "33333333-3333-4333-8333-333333333333",
            "type": "HKCategoryTypeIdentifierSleepAnalysis",
            "kind": "category",
            "start": start_ms - 3_600_000,
            "end": start_ms,
            "startContext": context,
            "endContext": context,
            "category": 4,
            "sourceName": "Apple Watch",
            "device": "Apple Watch",
        },
        {
            "uuid": "55555555-5555-4555-8555-555555555555",
            "type": "HKCategoryTypeIdentifierSleepAnalysis",
            "kind": "category",
            "start": start_ms - 3 * 3_600_000,
            "end": start_ms - 2 * 3_600_000,
            "startContext": context,
            "endContext": context,
            "category": 3,
            "sourceName": "Apple Watch",
            "device": "Apple Watch",
        },
        {
            "uuid": "66666666-6666-4666-8666-666666666666",
            "type": "HKCategoryTypeIdentifierSleepAnalysis",
            "kind": "category",
            "start": start_ms - 2 * 3_600_000,
            "end": start_ms - 1 * 3_600_000,
            "startContext": context,
            "endContext": context,
            "category": 5,
            "sourceName": "Apple Watch",
            "device": "Apple Watch",
        },
        {
            "uuid": "77777777-7777-4777-8777-777777777777",
            "type": "HKCategoryTypeIdentifierSleepAnalysis",
            "kind": "category",
            "start": start_ms - 3 * 3_600_000 - 30 * 60_000,
            "end": start_ms - 30 * 60_000,
            "startContext": context,
            "endContext": context,
            "category": 1,
            "sourceName": "Apple Watch",
            "device": "Apple Watch",
        },
        {
            "uuid": "44444444-4444-4444-8444-444444444444",
            "type": "HKQuantityTypeIdentifierDietaryWater",
            "kind": "quantity",
            "start": start_ms,
            "end": start_ms,
            "startContext": context,
            "endContext": context,
            "value": 250,
            "unit": "mL",
            "sourceName": "Other",
        },
        {
            "aggregate": {
                "type": "HKQuantityTypeIdentifierStepCount",
                "func": "sum",
                "intervalValue": 1,
                "intervalUnit": "day",
                "deviceFilter": "all",
                "bucketStart": start_ms - 23 * 3_600_000,
                "bucketEnd": start_ms + 3_600_000,
                "bucketStartContext": context,
                "bucketEndContext": context,
                "value": 321,
                "unit": "count",
            }
        },
    ]
    return ("\n".join(json.dumps(item, separators=(",", ":")) for item in lines) + "\n").encode()


def offset_batch(batch_id: str, offset_seconds: int, samples: list[dict], aggregates: list[dict]) -> str:
    """A synthetic Puls batch whose client clock is UTC+offset (not anyone's home zone)."""
    hours = -offset_seconds // 3600
    context = {
        "timeZoneID": f"Etc/GMT{hours:+d}",
        "utcOffsetSeconds": offset_seconds,
        "source": "device_current",
        "confidence": "inferred",
        "tzdbVersion": "",
    }
    header = {
        "schemaVersion": 1, "clientVersion": "test", "batchID": batch_id, "deviceID": "test-device",
        "type": "HKQuantityTypeIdentifierStepCount", "reason": "manual",
        "exportedAt": max([s["start"] for s in samples] + [a["bucketEnd"] for a in aggregates]) + 60_000,
        "sampleCount": len(samples), "deletionCount": 0, "routeCount": 0, "seriesCount": 0,
        "aggregateCount": len(aggregates), "activitySummaryCount": 0, "profileCount": 0,
    }
    lines = [header]
    for sample in samples:
        lines.append({"kind": "quantity", "end": sample["start"], "startContext": context, "endContext": context, **sample})
    for aggregate in aggregates:
        lines.append({"aggregate": {"func": "sum", "intervalValue": 1, "intervalUnit": "day", "deviceFilter": "all",
                                    "bucketStartContext": context, "bucketEndContext": context, **aggregate}})
    return "\n".join(json.dumps(item, separators=(",", ":")) for item in lines) + "\n"


def epoch_ms(text: str) -> int:
    return int(datetime.fromisoformat(text).timestamp() * 1000)


class ClientOffsetTests(unittest.TestCase):
    """Local days follow the phone's UTC offset, not the UTC calendar date."""

    def test_positive_offset_daily_aggregate_lands_on_the_local_day(self):
        # UTC+05:00: local midnight of 2026-10-06 is 2026-10-05T19:00Z (previous UTC date).
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            body = offset_batch(
                "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                5 * 3600,
                samples=[{
                    "uuid": "d1d1d1d1-d1d1-4d1d-8d1d-d1d1d1d1d1d1", "type": "HKQuantityTypeIdentifierStepCount",
                    "start": epoch_ms("2026-10-06T01:00:00+05:00"), "value": 100, "unit": "count",
                    "sourceName": "iPhone", "device": "iPhone",
                }],
                aggregates=[{
                    "type": "HKQuantityTypeIdentifierStepCount", "value": 4321, "unit": "count",
                    "bucketStart": epoch_ms("2026-10-06T00:00:00+05:00"),
                    "bucketEnd": epoch_ms("2026-10-07T00:00:00+05:00"),
                }],
            )
            ingest_puls_ndjson(data_dir, body, received_at=datetime(2026, 10, 6, 20, 0, tzinfo=timezone.utc))
            day = health_day(data_dir, "2026-10-06", now=datetime(2026, 10, 6, 20, 0, tzinfo=timezone.utc))
            self.assertEqual(day["activity"].get("step_count", {}).get("qty"), 4321)
            previous = health_day(data_dir, "2026-10-05", now=datetime(2026, 10, 6, 20, 0, tzinfo=timezone.utc))
            self.assertNotIn("step_count", previous["activity"])

    def test_negative_offset_late_evening_sample_stays_on_the_local_day(self):
        # UTC-05:00: 22:00 local on 2026-10-06 is 2026-10-07T03:00Z (next UTC date).
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            body = offset_batch(
                "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
                -5 * 3600,
                samples=[{
                    "uuid": "e1e1e1e1-e1e1-4e1e-8e1e-e1e1e1e1e1e1", "type": "HKQuantityTypeIdentifierHeartRate",
                    "start": epoch_ms("2026-10-06T22:00:00-05:00"), "value": 66, "unit": "count/min",
                    "sourceName": "Apple Watch", "device": "Apple Watch",
                }],
                aggregates=[],
            )
            ingest_puls_ndjson(data_dir, body, received_at=datetime(2026, 10, 7, 3, 5, tzinfo=timezone.utc))
            now = datetime(2026, 10, 7, 3, 10, tzinfo=timezone.utc)
            self.assertEqual((health_day(data_dir, "2026-10-06", now=now)["heart_rate"] or {}).get("count"), 1)
            self.assertFalse((health_day(data_dir, "2026-10-07", now=now)["heart_rate"] or {}).get("count"))


class StoreTests(unittest.TestCase):
    def test_ingest_snapshot_detail_and_upsert(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            received = datetime(2026, 9, 29, 11, 10, tzinfo=timezone.utc)
            result = ingest_hae_payload(data_dir, payload(), received_at=received)
            self.assertEqual(result["accepted"], 7)
            self.assertEqual(result["ignored"], 1)

            now = datetime(2026, 9, 29, 11, 30, tzinfo=timezone.utc)
            snap = health_now(data_dir, now=now)
            self.assertEqual(snap["status"], "ok")
            self.assertEqual(snap["freshness_seconds"], 20 * 60)
            self.assertEqual(snap["latest"]["heart_rate"]["value"]["avg"], 79)
            self.assertEqual(snap["today"]["step_count"]["qty"], 300)
            self.assertEqual(snap["today"]["active_energy"]["qty"], 52.75)
            self.assertEqual(snap["sleep"]["value"]["deep"], 1.0)
            self.assertNotIn("metadata", json.dumps(snap))

            # Same hour is an upsert, not a duplicate bucket.
            ingest_hae_payload(
                data_dir,
                payload(step_first=150),
                received_at=datetime(2026, 9, 29, 11, 20, tzinfo=timezone.utc),
            )
            snap2 = health_now(data_dir, now=now)
            self.assertEqual(snap2["today"]["step_count"]["qty"], 350)
            self.assertEqual(snap2["today"]["step_count"]["buckets"], 2)
            self.assertEqual(snap2["freshness_seconds"], 10 * 60)

            detail = health_detail(data_dir, "heart_rate", hours=2, now=now)
            self.assertEqual(detail["status"], "ok")
            self.assertEqual(len(detail["samples"]), 2)
            self.assertAlmostEqual(detail["summary"]["avg"], 75.5)

            day = health_day(data_dir, now=now)
            self.assertEqual(day["status"], "ok")
            self.assertEqual(day["date"], "2026-09-29")
            self.assertEqual(day["latest"]["heart_rate"]["value"]["avg"], 79)
            self.assertEqual(day["activity"]["step_count"]["qty"], 350)
            self.assertEqual(day["sleep"]["value"]["deep"], 1.0)
            self.assertEqual(day["heart_rate"]["count"], 2)
            self.assertAlmostEqual(day["heart_rate"]["avg"], 75.5)
            self.assertEqual(day["heart_rate"]["min"], 61.0)
            self.assertEqual(day["heart_rate"]["max"], 101.0)

            explicit_day = health_day(data_dir, "2026-09-29", now=now)
            self.assertEqual(explicit_day["date"], "2026-09-29")
            self.assertEqual(explicit_day["activity"]["active_energy"]["qty"], 52.75)

            beat = heartbeat(data_dir, now=now)
            self.assertEqual(beat["status"], "ok")
            self.assertEqual(beat["bpm"], 79)
            self.assertEqual(beat["min_bpm"], 64)
            self.assertEqual(beat["max_bpm"], 101)
            self.assertEqual(beat["age_seconds"], 30 * 60)
            self.assertEqual(beat["freshness_seconds"], 10 * 60)

    def test_activity_reads_normalize_mixed_units(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            mixed = {
                "data": {
                    "metrics": [
                        {
                            "name": "walking_running_distance",
                            "units": "m",
                            "data": [
                                {"date": "2026-10-07 08:00:00 +0000", "source": "Apple Watch", "qty": 1500},
                            ],
                        },
                        {
                            "name": "walking_running_distance",
                            "units": "km",
                            "data": [
                                {"date": "2026-10-07 09:00:00 +0000", "source": "Apple Watch", "qty": 0.5},
                            ],
                        },
                        {
                            "name": "active_energy",
                            "units": "kJ",
                            "data": [
                                {"date": "2026-10-07 08:00:00 +0000", "source": "Apple Watch", "qty": 418.4},
                            ],
                        },
                        {
                            "name": "active_energy",
                            "units": "kcal",
                            "data": [
                                {"date": "2026-10-07 09:00:00 +0000", "source": "Apple Watch", "qty": 100},
                            ],
                        },
                        {
                            "name": "apple_exercise_time",
                            "units": "hr",
                            "data": [
                                {"date": "2026-10-07 08:00:00 +0000", "source": "Apple Watch", "qty": 0.5},
                            ],
                        },
                        {
                            "name": "apple_exercise_time",
                            "units": "min",
                            "data": [
                                {"date": "2026-10-07 09:00:00 +0000", "source": "Apple Watch", "qty": 15},
                            ],
                        },
                    ]
                }
            }
            result = ingest_hae_payload(
                data_dir,
                mixed,
                received_at=datetime(2026, 10, 7, 9, 5, tzinfo=timezone.utc),
            )
            self.assertEqual(result["accepted"], 6)
            day = health_day(data_dir, "2026-10-07", now=datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc))
            self.assertEqual(day["activity"]["walking_running_distance"]["units"], "km")
            self.assertAlmostEqual(day["activity"]["walking_running_distance"]["qty"], 2.0)
            self.assertEqual(day["activity"]["active_energy"]["units"], "kcal")
            self.assertAlmostEqual(day["activity"]["active_energy"]["qty"], 200.0)
            self.assertEqual(day["activity"]["apple_exercise_time"]["units"], "min")
            self.assertAlmostEqual(day["activity"]["apple_exercise_time"]["qty"], 45.0)

    def test_activity_raw_fallback_uses_highest_single_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            payload = {
                "data": {
                    "metrics": [
                        {
                            "name": "step_count",
                            "units": "count",
                            "data": [
                                {"date": "2026-10-07 08:00:00 +0000", "source": "Apple Watch", "qty": 6000},
                                {"date": "2026-10-07 09:00:00 +0000", "source": "Apple Watch", "qty": 5800},
                                {"date": "2026-10-07 08:00:00 +0000", "source": "iPhone", "qty": 5700},
                                {"date": "2026-10-07 09:00:00 +0000", "source": "iPhone", "qty": 5600},
                            ],
                        }
                    ]
                }
            }
            ingest_hae_payload(
                data_dir,
                payload,
                received_at=datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc),
            )
            day = health_day(
                data_dir,
                "2026-10-07",
                now=datetime(2026, 10, 7, 11, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(day["activity"]["step_count"]["qty"], 11800)
            self.assertEqual(day["activity"]["step_count"]["buckets"], 2)

    def test_hourly_aggregate_plus_raw_tail_then_daily_final(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            initialize_store(data_dir)
            db = data_dir / "health.sqlite3"
            rows = [
                ("step_count", "2026-10-06T15:00:00Z", "2026-10-07", "PulsHealth aggregate:all:1:hour", "count", "aggregate", '{"bucket_end":"2026-10-06T16:00:00Z","device_filter":"all","func":"sum","interval_unit":"hour","interval_value":1,"qty":500}', "2026-10-06T18:00:00Z"),
                ("step_count", "2026-10-06T16:00:00Z", "2026-10-07", "PulsHealth aggregate:all:1:hour", "count", "aggregate", '{"bucket_end":"2026-10-06T17:00:00Z","device_filter":"all","func":"sum","interval_unit":"hour","interval_value":1,"qty":600}', "2026-10-06T18:00:00Z"),
                ("step_count", "2026-10-06T17:05:00Z", "2026-10-07", "Apple Watch", "count", "quantity", '{"qty":70}', "2026-10-06T18:00:00Z"),
                ("step_count", "2026-10-06T17:15:00Z", "2026-10-07", "Apple Watch", "count", "quantity", '{"qty":30}', "2026-10-06T18:00:00Z"),
                ("step_count", "2026-10-06T17:06:00Z", "2026-10-07", "iPhone", "count", "quantity", '{"qty":60}', "2026-10-06T18:00:00Z"),
                ("step_count", "2026-10-06T17:16:00Z", "2026-10-07", "iPhone", "count", "quantity", '{"qty":25}', "2026-10-06T18:00:00Z"),
            ]
            with sqlite3.connect(db) as conn:
                conn.executemany(
                    "INSERT INTO metric_samples(metric,observed_at,local_date,source,units,kind,payload_json,received_at) VALUES (?,?,?,?,?,?,?,?)",
                    rows,
                )
                conn.commit()

            live = health_day(data_dir, "2026-10-07", now=datetime(2026, 10, 7, 3, 30, tzinfo=timezone.utc))
            self.assertEqual(live["activity"]["step_count"]["qty"], 1200)
            self.assertEqual(live["activity"]["step_count"]["buckets"], 4)

            with sqlite3.connect(db) as conn:
                conn.execute(
                    "INSERT INTO metric_samples(metric,observed_at,local_date,source,units,kind,payload_json,received_at) VALUES (?,?,?,?,?,?,?,?)",
                    ("step_count", "2026-10-06T15:00:00Z", "2026-10-07", "PulsHealth aggregate:all:1:day", "count", "aggregate", '{"bucket_end":"2026-10-07T15:00:00Z","device_filter":"all","func":"sum","interval_unit":"day","interval_value":1,"qty":1180}', "2026-10-07T16:00:00Z"),
                )
                conn.commit()

            final = health_day(data_dir, "2026-10-07", now=datetime(2026, 10, 8, 1, 5, tzinfo=timezone.utc))
            self.assertEqual(final["activity"]["step_count"]["qty"], 1180)
            self.assertEqual(final["activity"]["step_count"]["buckets"], 1)

    def test_transition_reads_prefer_puls_over_legacy_hae(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            transition = {
                "data": {
                    "metrics": [
                        {
                            "name": "heart_rate",
                            "units": "count/min",
                            "data": [
                                {
                                    "date": "2026-10-06 22:59:00 +0000",
                                    "source": "Apple Watch",
                                    "Min": 180,
                                    "Avg": 200,
                                    "Max": 220,
                                }
                            ],
                        },
                        {
                            "name": "step_count",
                            "units": "count",
                            "data": [
                                {
                                    "date": "2026-10-06 22:59:00 +0000",
                                    "source": "Apple Watch",
                                    "qty": 500,
                                }
                            ],
                        },
                        {
                            "name": "sleep_analysis",
                            "units": "hr",
                            "data": [
                                {
                                    "date": "2026-10-06 23:00:00 +0000",
                                    "source": "Apple Watch",
                                    "deep": 5.0,
                                    "core": 5.0,
                                    "sleepStart": "2026-10-06 13:00:00 +0000",
                                    "sleepEnd": "2026-10-06 23:00:00 +0000",
                                }
                            ],
                        },
                    ]
                }
            }
            ingest_hae_payload(
                data_dir,
                transition,
                received_at=datetime(2026, 10, 6, 23, 0, tzinfo=timezone.utc),
            )

            lines = [json.loads(line) for line in puls_batch().decode().splitlines() if line.strip()]
            lines[0]["aggregateCount"] = 0
            puls_raw_only = chr(10).join(json.dumps(item, separators=(",", ":")) for item in lines[:-1]) + chr(10)
            ingest_puls_ndjson(
                data_dir,
                puls_raw_only,
                received_at=datetime(2026, 10, 6, 23, 5, tzinfo=timezone.utc),
            )

            day = health_day(
                data_dir,
                "2026-10-06",
                now=datetime(2026, 10, 7, 0, 30, tzinfo=timezone.utc),
            )
            self.assertEqual(day["activity"]["step_count"]["qty"], 100)
            self.assertEqual(day["heart_rate"]["count"], 1)
            self.assertEqual(day["heart_rate"]["latest"], 81)
            self.assertEqual(day["heart_rate"]["avg"], 81)
            # Sleep follows the same rule: the PulsHealth session wins over a legacy HAE nightly aggregate.
            self.assertEqual(day["sleep"]["value"]["deep"], 1.0)
            self.assertNotEqual(day["sleep"]["value"].get("core"), 5.0)

    def test_sleep_falls_back_to_legacy_hae_when_puls_has_no_sleep_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            legacy = {"data": {"metrics": [{"name": "sleep_analysis", "units": "hr", "data": [{
                "date": "2026-10-06 23:00:00 +0000", "source": "Apple Watch", "deep": 5.0, "core": 5.0,
                "sleepStart": "2026-10-06 13:00:00 +0000", "sleepEnd": "2026-10-06 23:00:00 +0000"}]}]}}
            ingest_hae_payload(data_dir, legacy, received_at=datetime(2026, 10, 6, 23, 0, tzinfo=timezone.utc))

            # Puls segments for the same night, but only in-bed (0) and awake (2): no sleep session.
            lines = [json.loads(line) for line in puls_batch().decode().splitlines() if line.strip()]
            header = dict(lines[0], sampleCount=2, aggregateCount=0)
            sleep = [dict(item) for item in lines[1:] if item.get("type") == "HKCategoryTypeIdentifierSleepAnalysis"][:2]
            sleep[0]["category"], sleep[1]["category"] = 0, 2
            body = "\n".join(json.dumps(item, separators=(",", ":")) for item in [header, *sleep]) + "\n"
            ingest_puls_ndjson(data_dir, body, received_at=datetime(2026, 10, 6, 23, 5, tzinfo=timezone.utc))

            day = health_day(data_dir, "2026-10-06", now=datetime(2026, 10, 7, 0, 30, tzinfo=timezone.utc))
            self.assertEqual(day["sleep"]["value"]["deep"], 5.0)

    def test_source_display_rename_is_deduped_and_existing_rows_migrate(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            initialize_store(data_dir)
            db = data_dir / "health.sqlite3"
            with sqlite3.connect(db) as conn:
                conn.execute("DELETE FROM sync_state WHERE key='source_identity_version'")
                conn.execute("DELETE FROM metric_samples")
                rows = [
                    (
                        "heart_rate",
                        "2026-09-30T00:45:00Z",
                        "2026-09-30",
                        "MikaのApple Watch",
                        "count/min",
                        "range",
                        '{"avg":90,"max":90,"min":90}',
                        "2026-09-30T00:15:00Z",
                    ),
                    (
                        "heart_rate",
                        "2026-09-30T00:45:00Z",
                        "2026-09-30",
                        "AlexのApple Watch",
                        "count/min",
                        "range",
                        '{"avg":90,"max":90,"min":90}',
                        "2026-09-30T02:20:00Z",
                    ),
                ]
                conn.executemany(
                    """
                    INSERT INTO metric_samples
                        (metric, observed_at, local_date, source, units, kind, payload_json, received_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO sync_state(key, value) VALUES('last_sync_at', '2026-09-30T02:20:00Z')"
                )
                conn.commit()

            initialize_store(data_dir)
            now = datetime(2026, 9, 30, 2, 30, tzinfo=timezone.utc)
            detail = health_detail(data_dir, "heart_rate", hours=6, now=now)
            self.assertEqual(len(detail["samples"]), 1)
            self.assertEqual(detail["summary"]["count"], 1)
            self.assertEqual(detail["summary"]["avg"], 90)
            self.assertEqual(detail["samples"][0]["source"], "Apple Watch")
            self.assertEqual(detail["samples"][0]["received_at"], "2026-09-30T02:20:00Z")
            day = health_day(data_dir, "2026-09-30", now=now)
            self.assertEqual(day["heart_rate"]["count"], 1)

            with sqlite3.connect(db) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM metric_samples WHERE metric='heart_rate'"
                ).fetchone()[0]
                version = conn.execute(
                    "SELECT value FROM sync_state WHERE key='source_identity_version'"
                ).fetchone()[0]
            self.assertEqual(count, 1)
            self.assertEqual(version, "2")

            renamed = {
                "data": {
                    "metrics": [
                        {
                            "name": "heart_rate",
                            "units": "count/min",
                            "data": [
                                {
                                    "date": "2026-09-30 00:45:00 +0000",
                                    "source": "another owner’s Apple Watch",
                                    "Min": 90,
                                    "Avg": 90,
                                    "Max": 90,
                                }
                            ],
                        }
                    ]
                }
            }
            ingest_hae_payload(
                data_dir,
                renamed,
                received_at=datetime(2026, 9, 30, 2, 25, tzinfo=timezone.utc),
            )
            detail_after = health_detail(data_dir, "heart_rate", hours=6, now=now)
            self.assertEqual(len(detail_after["samples"]), 1)
            self.assertEqual(detail_after["summary"]["count"], 1)

    def test_empty_and_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            self.assertEqual(health_now(data_dir)["status"], "empty")
            with self.assertRaises(HealthPayloadError):
                ingest_hae_payload(data_dir, {"nope": 1})
            with self.assertRaises(ValueError):
                health_detail(data_dir, "dietary_water")
            with self.assertRaises(ValueError):
                health_day(data_dir, "2026-02-31")
            self.assertEqual(heartbeat(data_dir)["status"], "empty")


class HttpTests(unittest.TestCase):
    def test_http_auth_and_ingest(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = ServiceSettings(
                host="127.0.0.1",
                port=0,
                data_dir=Path(tmp),
                token="a" * 64,
                max_body_bytes=1024 * 1024,
            )
            server = build_http_server(settings)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                body = json.dumps(payload()).encode()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request(
                    "POST",
                    "/api/health-ingest",
                    body=body,
                    headers={"Content-Type": "application/json"},
                )
                response = conn.getresponse()
                self.assertEqual(response.status, 410)
                response.read()
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request(
                    "POST",
                    "/api/health-ingest",
                    body=body,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": "Bearer " + settings.token,
                    },
                )
                response = conn.getresponse()
                parsed = json.loads(response.read())
                self.assertEqual(response.status, 410)
                self.assertEqual(parsed["error"], "hae_ingest_retired")
                conn.close()

                # Historic HAE records stay readable; only HTTP write is
                # retired. The test seeds a trusted local fixture.
                ingest_hae_payload(Path(tmp), payload())
                self.assertEqual(health_now(Path(tmp))["status"], "ok")

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("GET", "/v1/health/now")
                response = conn.getresponse()
                now_payload = json.loads(response.read())
                self.assertEqual(response.status, 200)
                self.assertEqual(now_payload["status"], "ok")
                self.assertEqual(now_payload["latest"]["heart_rate"]["value"]["avg"], 79)
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("GET", "/v1/health/detail?metric=heart_rate&hours=48&limit=10")
                response = conn.getresponse()
                detail_payload = json.loads(response.read())
                self.assertEqual(response.status, 200)
                self.assertEqual(detail_payload["metric"], "heart_rate")
                self.assertIn(detail_payload["status"], {"ok", "empty"})
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("GET", "/v1/health/day")
                response = conn.getresponse()
                day_payload = json.loads(response.read())
                self.assertEqual(response.status, 200)
                self.assertEqual(day_payload["date"], "2026-09-29")
                self.assertEqual(day_payload["activity"]["step_count"]["qty"], 300)
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("GET", "/v1/health/day?date=2026-09-29")
                response = conn.getresponse()
                explicit_day_payload = json.loads(response.read())
                self.assertEqual(response.status, 200)
                self.assertEqual(explicit_day_payload["heart_rate"]["latest"], 79)
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("GET", "/v1/health/day?date=2026-02-31")
                response = conn.getresponse()
                self.assertEqual(response.status, 400)
                response.read()
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("GET", "/v1/health/heartbeat")
                response = conn.getresponse()
                heartbeat_payload = json.loads(response.read())
                self.assertEqual(response.status, 200)
                self.assertEqual(heartbeat_payload["bpm"], 79)
                self.assertEqual(heartbeat_payload["source"], "Apple Watch")
                conn.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


    def test_pulshealth_protocol_ingest_and_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = ServiceSettings(
                host="127.0.0.1",
                port=0,
                data_dir=Path(tmp),
                token="b" * 64,
                max_body_bytes=1024 * 1024,
            )
            server = build_http_server(settings)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("GET", "/api/health-sync/v1/capabilities")
                response = conn.getresponse()
                self.assertEqual(response.status, 401)
                response.read()
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request(
                    "GET",
                    "/api/health-sync/v1/capabilities",
                    headers={
                        "Authorization": "Bearer " + settings.token,
                        "X-Puls-Protocol": "1",
                    },
                )
                response = conn.getresponse()
                capabilities = json.loads(response.read())
                self.assertEqual(response.status, 200)
                self.assertEqual(capabilities["protocolVersions"], [1])
                self.assertIn("aggregates", capabilities["features"])
                conn.close()

                body = gzip.compress(puls_batch())
                headers = {
                    "Authorization": "Bearer " + settings.token,
                    "Content-Type": "application/x-ndjson",
                    "Content-Encoding": "gzip",
                    "X-Puls-Protocol": "1",
                    "X-User-ID": "5ea4d000-0000-4000-8000-000000000001",
                }
                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("POST", "/api/health-sync/v1/batches", body=body, headers=headers)
                response = conn.getresponse()
                result = json.loads(response.read())
                self.assertEqual(response.status, 200)
                self.assertEqual(result["accepted"], 6)
                self.assertEqual(result["duplicates"], 0)
                self.assertEqual(result["aggregateSamples"], 1)
                conn.close()

                now = datetime(2026, 10, 7, 0, 30, tzinfo=timezone.utc)
                snap = health_now(Path(tmp), now=now)
                self.assertEqual(snap["latest"]["heart_rate"]["value"]["qty"], 81)
                self.assertEqual(snap["latest"]["heart_rate"]["source"], "Apple Watch")
                self.assertEqual(snap["today"]["step_count"]["qty"], 321)
                self.assertEqual(snap["today"]["step_count"]["buckets"], 1)
                self.assertEqual(snap["sleep"]["units"], "hr")
                self.assertEqual(snap["sleep"]["source"], "Apple Watch")
                self.assertAlmostEqual(snap["sleep"]["value"]["core"], 1.0)
                self.assertAlmostEqual(snap["sleep"]["value"]["rem"], 1.0)
                self.assertAlmostEqual(snap["sleep"]["value"]["deep"], 1.0)
                self.assertNotIn("asleep", snap["sleep"]["value"])
                puls_day = health_day(Path(tmp), "2026-10-06", now=now)
                self.assertAlmostEqual(puls_day["sleep"]["value"]["core"], 1.0)
                self.assertAlmostEqual(puls_day["sleep"]["value"]["rem"], 1.0)
                self.assertAlmostEqual(puls_day["sleep"]["value"]["deep"], 1.0)

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request("POST", "/api/health-sync/v1/batches", body=body, headers=headers)
                response = conn.getresponse()
                replay = json.loads(response.read())
                self.assertEqual(response.status, 200)
                self.assertEqual(replay["accepted"], 0)
                self.assertEqual(replay["duplicates"], 7)
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=3)
                conn.request(
                    "POST",
                    "/api/health-sync/v1/batches",
                    body=body,
                    headers={**headers, "X-Puls-Protocol": "2"},
                )
                response = conn.getresponse()
                bad_version = json.loads(response.read())
                self.assertEqual(response.status, 400)
                self.assertEqual(bad_version["supportedVersions"], [1])
                conn.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
