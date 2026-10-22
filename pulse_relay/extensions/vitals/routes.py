"""Loopback GET routes the core server mounts when this extension is installed."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs

from .status import read_vitals_status
from .outbox import read_vitals_outbox

Route = Callable[[Path, str], tuple[int, dict[str, object]]]


def _limit(query: str, default: str) -> int:
    params = parse_qs(query, keep_blank_values=True)
    if set(params) - {"limit"} or len(params.get("limit", [])) > 1:
        raise ValueError("unexpected query")
    raw = (params.get("limit") or [default])[0]
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError("limit must be decimal")
    return int(raw)


def vitals_outbox(data_dir: Path, query: str) -> tuple[int, dict[str, object]]:
    try:
        result = read_vitals_outbox(data_dir, now=datetime.now(timezone.utc), limit=_limit(query, "50"))
    except ValueError:
        return 400, {"error": "invalid_query"}
    return 200, result


def vitals(data_dir: Path, query: str) -> tuple[int, dict[str, object]]:
    try:
        result = read_vitals_status(data_dir, limit=_limit(query, "30"))
    except ValueError:
        return 400, {"error": "invalid_query"}
    return 200, result


ROUTES: dict[str, Route] = {
    "/v1/health/vitals-outbox": vitals_outbox,
    "/v1/health/vitals": vitals,
}
