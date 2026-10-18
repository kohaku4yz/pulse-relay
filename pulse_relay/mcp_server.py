"""Dedicated, loopback-only MCP exposing exactly two owner-health tools to an AI agent."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final
from urllib.parse import urlsplit

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from .health_client import (
    HealthClientSettings,
    HealthDetailResult,
    HeartbeatResult,
    health_detail as read_health_detail,
    heartbeat as read_heartbeat,
)

SERVICE_NAME: Final[str] = "pulse-relay-mcp"
DEFAULT_HOST: Final[str] = "127.0.0.1"
DEFAULT_PORT: Final[int] = 8879
DEFAULT_PATH: Final[str] = "/mcp"
DEFAULT_HEALTH_SERVICE_URL: Final[str] = "http://127.0.0.1:8768"
_LOOPBACK_HOSTS: Final[frozenset[str]] = frozenset({"127.0.0.1", "localhost", "::1"})

INSTRUCTIONS = """Read-only owner health context backed by the pulse-relay server.
Use health_detail for one local calendar day. With no date it returns the latest day with data.
Use heartbeat for the latest available heart-rate sample. Always inspect age_seconds (when the sample
was measured) and freshness_seconds (when the service last synced) before describing a value as current.
Empty or unavailable is not zero.
""".strip()


class McpSettingsError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class McpSettings:
    host: str
    port: int
    path: str
    health_service_url: str


def _loopback_http_base(value: str) -> str:
    text = value.strip().rstrip("/")
    parts = urlsplit(text)
    if parts.scheme != "http" or parts.hostname not in _LOOPBACK_HOSTS:
        raise McpSettingsError("PULSE_RELAY_SERVICE_URL must be loopback http")
    if parts.username or parts.password or parts.query or parts.fragment:
        raise McpSettingsError("PULSE_RELAY_SERVICE_URL must be a clean loopback base URL")
    if parts.path not in {"", "/"}:
        raise McpSettingsError("PULSE_RELAY_SERVICE_URL must not contain a path")
    return text


def load_settings(*, env: dict[str, str] | None = None) -> McpSettings:
    raw = os.environ if env is None else env
    host = raw.get("PULSE_RELAY_MCP_HOST", DEFAULT_HOST).strip()
    if host not in _LOOPBACK_HOSTS:
        raise McpSettingsError("PULSE_RELAY_MCP_HOST must be loopback")
    try:
        port = int(raw.get("PULSE_RELAY_MCP_PORT", str(DEFAULT_PORT)))
    except ValueError:
        raise McpSettingsError("PULSE_RELAY_MCP_PORT must be an integer") from None
    if not 1 <= port <= 65535:
        raise McpSettingsError("PULSE_RELAY_MCP_PORT is out of range")
    path = raw.get("PULSE_RELAY_MCP_PATH", DEFAULT_PATH).strip()
    if not path.startswith("/") or "?" in path or "#" in path or (path.endswith("/") and path != "/"):
        raise McpSettingsError("PULSE_RELAY_MCP_PATH must be a clean absolute path")
    service_url = _loopback_http_base(raw.get("PULSE_RELAY_SERVICE_URL", DEFAULT_HEALTH_SERVICE_URL))
    return McpSettings(host=host, port=port, path=path, health_service_url=service_url)


def _transport_security(cfg: McpSettings) -> TransportSecuritySettings:
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            f"127.0.0.1:{cfg.port}",
            f"localhost:{cfg.port}",
            f"[::1]:{cfg.port}",
        ],
        allowed_origins=[
            f"http://127.0.0.1:{cfg.port}",
            f"http://localhost:{cfg.port}",
        ],
    )


def build_server(settings: McpSettings | None = None) -> FastMCP:
    cfg = settings if settings is not None else load_settings()
    client = HealthClientSettings(service_url=cfg.health_service_url)
    server = FastMCP(
        name=SERVICE_NAME,
        instructions=INSTRUCTIONS,
        host=cfg.host,
        port=cfg.port,
        streamable_http_path=cfg.path,
        stateless_http=True,
        json_response=True,
        log_level="WARNING",
        transport_security=_transport_security(cfg),
    )

    @server.tool(
        name="health_detail",
        description=(
            "Read compact owner health context for one local calendar day. "
            "Pass date as YYYY-MM-DD; omit it to get the latest day with available data."
        ),
    )
    async def health_detail_tool(date: str | None = None) -> HealthDetailResult:
        return await read_health_detail(client, date)

    @server.tool(
        name="heartbeat",
        description=(
            "Read the latest available heart-rate measurement with only the freshness metadata "
            "needed to judge whether it is current. Takes no arguments."
        ),
    )
    async def heartbeat_tool() -> HeartbeatResult:
        return await read_heartbeat(client)

    return server


def main() -> int:
    build_server().run(transport="streamable-http")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["McpSettings", "McpSettingsError", "build_server", "load_settings", "main"]
