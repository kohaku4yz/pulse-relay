"""Compact loopback client for the pulse-relay server.

The output shape is the shared contract for every agent adapter (the MCP server here, and the
health tools in haru-vps-mcp): keep the two copies identical.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

import httpx


class HealthDetailResult(TypedDict):
    status: str
    date: str | None
    freshness_seconds: int | None
    latest: dict[str, Any]
    activity: dict[str, Any]
    sleep: dict[str, Any] | None
    heart_rate: dict[str, Any] | None


class HeartbeatResult(TypedDict):
    status: str
    bpm: float | int | None
    observed_at: str | None
    age_seconds: int | None
    freshness_seconds: int | None


@dataclass(frozen=True, slots=True)
class HealthClientSettings:
    service_url: str
    timeout_seconds: float = 3.0


def _clean_optional_text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _clean_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _clean_number(value: object) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def _compact_latest(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    compact: dict[str, Any] = {}
    for metric, item in raw.items():
        if not isinstance(metric, str) or not isinstance(item, dict):
            continue
        value = item.get("value")
        if not isinstance(value, dict):
            continue
        entry = dict(value)
        units = _clean_optional_text(item.get("units"))
        age = _clean_nonnegative_int(item.get("age_seconds"))
        if units:
            entry["units"] = units
        if age is not None:
            entry["age_seconds"] = age
        compact[metric] = entry
    return compact


def _compact_activity(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    compact: dict[str, Any] = {}
    for metric, item in raw.items():
        if not isinstance(metric, str) or not isinstance(item, dict):
            continue
        qty = _clean_number(item.get("qty"))
        units = _clean_optional_text(item.get("units"))
        if qty is None:
            continue
        entry: dict[str, Any] = {"qty": qty}
        if units:
            entry["units"] = units
        compact[metric] = entry
    return compact


def _compact_sleep(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    value = raw.get("value")
    if not isinstance(value, dict):
        return None
    compact = dict(value)
    units = _clean_optional_text(raw.get("units"))
    if units:
        compact["units"] = units
    return compact


def _compact_heart_rate(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    allowed = ("count", "units", "latest", "last_observed_at", "avg", "min", "max")
    compact = {key: raw[key] for key in allowed if key in raw}
    return compact or None


def _unavailable_detail(date: str | None) -> HealthDetailResult:
    return {
        "status": "unavailable",
        "date": date,
        "freshness_seconds": None,
        "latest": {},
        "activity": {},
        "sleep": None,
        "heart_rate": None,
    }


def _unavailable_heartbeat() -> HeartbeatResult:
    return {
        "status": "unavailable",
        "bpm": None,
        "observed_at": None,
        "age_seconds": None,
        "freshness_seconds": None,
    }


async def _get_json(
    settings: HealthClientSettings,
    path: str,
    params: dict[str, object] | None = None,
) -> dict[str, Any] | None:
    url = settings.service_url.rstrip("/") + path
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(settings.timeout_seconds),
            follow_redirects=False,
            trust_env=False,
        ) as client:
            response = await client.get(url, params=params)
            if response.status_code == 400:
                raise ValueError("pulse-relay rejected the query")
            if response.status_code != 200:
                return None
            payload = response.json()
    except ValueError:
        raise
    except (httpx.HTTPError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


async def health_detail(settings: HealthClientSettings, date: str | None = None) -> HealthDetailResult:
    params = {"date": date} if date is not None else None
    payload = await _get_json(settings, "/v1/health/day", params)
    if payload is None:
        return _unavailable_detail(date)

    status = payload.get("status")
    return {
        "status": status if isinstance(status, str) else "unavailable",
        "date": _clean_optional_text(payload.get("date")),
        "freshness_seconds": _clean_nonnegative_int(payload.get("freshness_seconds")),
        "latest": _compact_latest(payload.get("latest")),
        "activity": _compact_activity(payload.get("activity")),
        "sleep": _compact_sleep(payload.get("sleep")),
        "heart_rate": _compact_heart_rate(payload.get("heart_rate")),
    }


async def heartbeat(settings: HealthClientSettings) -> HeartbeatResult:
    payload = await _get_json(settings, "/v1/health/heartbeat")
    if payload is None:
        return _unavailable_heartbeat()

    status = payload.get("status")
    return {
        "status": status if isinstance(status, str) else "unavailable",
        "bpm": _clean_number(payload.get("bpm")),
        "observed_at": _clean_optional_text(payload.get("observed_at")),
        "age_seconds": _clean_nonnegative_int(payload.get("age_seconds")),
        "freshness_seconds": _clean_nonnegative_int(payload.get("freshness_seconds")),
    }


__all__ = [
    "HealthClientSettings",
    "HealthDetailResult",
    "HeartbeatResult",
    "health_detail",
    "heartbeat",
]
