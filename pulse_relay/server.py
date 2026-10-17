"""Loopback-only shared health service; PulsHealth sync is the only ingest route."""
from __future__ import annotations

import hmac
import importlib
import importlib.util
import json
import pkgutil
import logging
import os
import zlib
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Final
from urllib.parse import parse_qs, urlsplit

from .store import (
    PulsPayloadError,
    PulsProtocolVersionError,
    health_day,
    health_detail,
    health_now,
    heartbeat,
    ingest_puls_ndjson,
    initialize_store,
)

logger = logging.getLogger(__name__)

DEFAULT_HOST: Final[str] = "127.0.0.1"
DEFAULT_PORT: Final[int] = 8768
# The retired Health Auto Export route answers 410 before reading the body.
RETIRED_HAE_INGEST_PATH: Final[str] = "/api/health-ingest"
DEFAULT_NOW_PATH: Final[str] = "/v1/health/now"
DEFAULT_DETAIL_PATH: Final[str] = "/v1/health/detail"
DEFAULT_DAY_PATH: Final[str] = "/v1/health/day"
DEFAULT_HEARTBEAT_PATH: Final[str] = "/v1/health/heartbeat"
DEFAULT_PULS_BASE_PATH: Final[str] = "/api/health-sync"
DEFAULT_PULS_BATCH_PATH: Final[str] = DEFAULT_PULS_BASE_PATH + "/v1/batches"
DEFAULT_PULS_CAPABILITIES_PATH: Final[str] = DEFAULT_PULS_BASE_PATH + "/v1/capabilities"
DEFAULT_DATA_DIR: Final[str] = "/var/lib/pulse-relay"
DEFAULT_MAX_PULS_DECODED_BYTES: Final[int] = 50 * 1024 * 1024
DEFAULT_MAX_BODY_BYTES: Final[int] = 5 * 1024 * 1024
_LOOPBACK_HOSTS: Final[frozenset[str]] = frozenset({"127.0.0.1", "localhost", "::1"})


class ServiceSettingsError(ValueError):
    """Raised when service configuration is invalid."""


@dataclass(frozen=True)
class ServiceSettings:
    host: str
    port: int
    data_dir: Path
    token: str
    max_body_bytes: int


def load_settings(*, env: dict[str, str] | None = None) -> ServiceSettings:
    raw = os.environ if env is None else env
    host = raw.get("PULSE_RELAY_HOST", DEFAULT_HOST).strip()
    if host not in _LOOPBACK_HOSTS:
        raise ServiceSettingsError("PULSE_RELAY_HOST must be loopback")

    try:
        port = int(raw.get("PULSE_RELAY_PORT", str(DEFAULT_PORT)))
    except ValueError:
        raise ServiceSettingsError("PULSE_RELAY_PORT must be an integer") from None
    if not 1 <= port <= 65535:
        raise ServiceSettingsError("PULSE_RELAY_PORT is out of range")

    data_dir = Path(raw.get("PULSE_RELAY_DATA_DIR", DEFAULT_DATA_DIR))
    if not data_dir.is_absolute():
        raise ServiceSettingsError("PULSE_RELAY_DATA_DIR must be absolute")

    token = raw.get("PULSE_RELAY_INGEST_TOKEN", "").strip()
    if len(token) < 32:
        raise ServiceSettingsError("PULSE_RELAY_INGEST_TOKEN must contain at least 32 characters")

    try:
        max_body = int(raw.get("PULSE_RELAY_MAX_BODY_BYTES", str(DEFAULT_MAX_BODY_BYTES)))
    except ValueError:
        raise ServiceSettingsError("PULSE_RELAY_MAX_BODY_BYTES must be an integer") from None
    if not 1024 <= max_body <= 50 * 1024 * 1024:
        raise ServiceSettingsError("PULSE_RELAY_MAX_BODY_BYTES must be between 1 KiB and 50 MiB")

    return ServiceSettings(host, port, data_dir, token, max_body)


def _candidate_token(handler: BaseHTTPRequestHandler) -> str:
    auth = handler.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return handler.headers.get("X-Health-Token", "").strip()




def _decode_puls_body(raw: bytes, encoding: str) -> str:
    normalized = encoding.strip().lower()
    if normalized in ("", "identity"):
        if len(raw) > DEFAULT_MAX_PULS_DECODED_BYTES:
            raise PulsPayloadError("decoded body too large")
        decoded = raw
    elif normalized == "gzip":
        inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            decoded = inflater.decompress(raw, DEFAULT_MAX_PULS_DECODED_BYTES + 1)
            if len(decoded) > DEFAULT_MAX_PULS_DECODED_BYTES:
                raise PulsPayloadError("decoded body too large")
            if not inflater.eof:
                raise PulsPayloadError("invalid gzip body")
            if inflater.unused_data:
                raise PulsPayloadError("multiple gzip members are not supported")
        except zlib.error as exc:
            raise PulsPayloadError("invalid gzip body") from exc
    else:
        raise PulsPayloadError("unsupported content encoding")
    try:
        return decoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PulsPayloadError("body must be utf-8") from exc


def _extension_routes() -> dict[str, object]:
    """Mount GET routes from every package under extensions/ that has a routes module.

    Optional add-ons; the core runs without the extensions/ directory. A present
    but broken extension, or two extensions claiming one path, fails at startup.
    """
    if importlib.util.find_spec(f"{__package__}.extensions") is None:
        return {}
    extensions = importlib.import_module(f"{__package__}.extensions")
    routes: dict[str, object] = {}
    for info in sorted(pkgutil.iter_modules(extensions.__path__), key=lambda item: item.name):
        if not info.ispkg:
            continue
        module = f"{extensions.__name__}.{info.name}.routes"
        if importlib.util.find_spec(module) is None:
            continue
        for path, route in importlib.import_module(module).ROUTES.items():
            if path in routes:
                raise RuntimeError(f"extension route {path} is defined twice")
            routes[path] = route
    return routes


def _handler(settings: ServiceSettings):
    extension_routes = _extension_routes()

    class HealthServiceHandler(BaseHTTPRequestHandler):
        server_version = "PulseRelay/1"
        sys_version = ""

        def log_message(self, format: str, *args) -> None:
            return

        def _send_json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parts = urlsplit(self.path)
            if parts.path == "/healthz":
                self._send_json(200, {"status": "ok"})
                return
            if parts.path == DEFAULT_PULS_CAPABILITIES_PATH:
                supplied = _candidate_token(self)
                if not supplied or not hmac.compare_digest(supplied, settings.token):
                    self._send_json(401, {"error": "unauthorized"})
                    return
                proto = self.headers.get("X-Puls-Protocol", "").strip()
                if proto and proto != "1":
                    self._send_json(400, {"error": "unsupported protocol version", "supportedVersions": [1]})
                    return
                self._send_json(
                    200,
                    {
                        "protocolVersions": [1],
                        "features": ["batches", "aggregates"],
                        "server": "pulse-relay",
                        "version": "1",
                    },
                )
                return
            if parts.path == DEFAULT_NOW_PATH:
                self._send_json(200, dict(health_now(settings.data_dir)))
                return
            if parts.path == DEFAULT_DAY_PATH:
                query = parse_qs(parts.query)
                raw_date = (query.get("date") or [None])[0]
                try:
                    result = health_day(settings.data_dir, raw_date)
                except ValueError:
                    self._send_json(400, {"error": "invalid_query"})
                    return
                self._send_json(200, dict(result))
                return
            if parts.path == DEFAULT_HEARTBEAT_PATH:
                self._send_json(200, dict(heartbeat(settings.data_dir)))
                return
            if parts.path == DEFAULT_DETAIL_PATH:
                query = parse_qs(parts.query)
                metric = (query.get("metric") or [""])[0]
                try:
                    hours = int((query.get("hours") or ["2"])[0])
                    limit = int((query.get("limit") or ["120"])[0])
                    result = health_detail(
                        settings.data_dir,
                        metric,
                        hours=hours,
                        limit=limit,
                    )
                except (ValueError, TypeError):
                    self._send_json(400, {"error": "invalid_query"})
                    return
                self._send_json(200, dict(result))
                return
            route = extension_routes.get(parts.path)
            if route is not None:
                status, payload = route(settings.data_dir, parts.query)
                self._send_json(status, payload)
                return
            self._send_json(404, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            parts = urlsplit(self.path)
            # Retired Health Auto Export lane: fail closed before reading
            # old requests. Historic data and PulsHealth sync remain intact.
            if parts.path == RETIRED_HAE_INGEST_PATH:
                self._send_json(410, {"error": "hae_ingest_retired"})
                return
            if parts.path != DEFAULT_PULS_BATCH_PATH:
                self._send_json(404, {"error": "not_found"})
                return

            supplied = _candidate_token(self)
            if not supplied or not hmac.compare_digest(supplied, settings.token):
                logger.warning("event=health_ingest result=unauthorized path=%s", parts.path)
                self._send_json(401, {"error": "unauthorized"})
                return

            if parts.path == DEFAULT_PULS_BATCH_PATH:
                proto = self.headers.get("X-Puls-Protocol", "").strip()
                if proto and proto != "1":
                    self._send_json(400, {"error": "unsupported protocol version", "supportedVersions": [1]})
                    return

            raw_length = self.headers.get("Content-Length")
            if raw_length is None:
                self._send_json(411, {"error": "content_length_required"})
                return
            try:
                length = int(raw_length)
            except ValueError:
                self._send_json(400, {"error": "invalid_content_length"})
                return
            if length < 0 or length > settings.max_body_bytes:
                self._send_json(413, {"error": "payload_too_large"})
                return

            body = self.rfile.read(length)
            try:
                text = _decode_puls_body(body, self.headers.get("Content-Encoding", "identity"))
                result = ingest_puls_ndjson(settings.data_dir, text)
            except PulsProtocolVersionError:
                self._send_json(400, {"error": "unsupported protocol version", "supportedVersions": [1]})
                return
            except PulsPayloadError as exc:
                logger.warning("event=health_ingest result=invalid_puls reason=%s", type(exc).__name__)
                self._send_json(400, {"error": str(exc)[:200] or "invalid_payload"})
                return
            except Exception:
                logger.exception("event=health_ingest result=internal_error transport=puls")
                self._send_json(500, {"error": "internal_error"})
                return

            logger.info(
                "event=health_ingest result=ok transport=puls accepted=%s ignored=%s duplicates=%s aggregates=%s",
                result["accepted"],
                result["ignored"],
                result["duplicates"],
                result["aggregateSamples"],
            )
            self._send_json(
                200,
                {
                    "accepted": result["accepted"],
                    "duplicates": result["duplicates"],
                    "deleted": result["deleted"],
                    "routePoints": 0,
                    "seriesPoints": 0,
                    "aggregateSamples": result["aggregateSamples"],
                    "activitySummaries": 0,
                },
            )


    return HealthServiceHandler


def build_http_server(settings: ServiceSettings) -> ThreadingHTTPServer:
    initialize_store(settings.data_dir)
    server = ThreadingHTTPServer((settings.host, settings.port), _handler(settings))
    server.daemon_threads = True
    return server


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    settings = load_settings()
    logger.info("event=pulse_relay_startup status=starting")
    server = build_http_server(settings)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
