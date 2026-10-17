#!/usr/bin/env python3
"""Real MCP protocol smoke test for the pulse-relay health MCP."""
from __future__ import annotations

import asyncio
import sys

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

URL = "http://127.0.0.1:8879/mcp"
EXPECTED_TOOLS = ["health_detail", "heartbeat"]
# The compact output contract shared with haru-vps-mcp's health tools.
DETAIL_KEYS = {"status", "date", "freshness_seconds", "latest", "activity", "sleep", "heart_rate"}
HEARTBEAT_KEYS = {"status", "bpm", "observed_at", "age_seconds", "freshness_seconds"}


def _nullable(schema: dict[str, object], field: str) -> bool:
    props = schema.get("properties")
    if not isinstance(props, dict):
        return False
    item = props.get(field)
    if not isinstance(item, dict):
        return False
    any_of = item.get("anyOf")
    return isinstance(any_of, list) and {"type": "null"} in any_of


async def run() -> int:
    failures: list[str] = []
    try:
        async with httpx.AsyncClient(trust_env=False) as http_client:
            async with streamable_http_client(URL, http_client=http_client) as (read_stream, write_stream, _):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    listed = await session.list_tools()
                    names = sorted(tool.name for tool in listed.tools)
                    print(f"tool discovery: {names}")
                    if names != EXPECTED_TOOLS:
                        failures.append(f"expected exactly {EXPECTED_TOOLS}, got {names}")

                    by_name = {tool.name: tool for tool in listed.tools}
                    detail_tool = by_name.get("health_detail")
                    heartbeat_tool = by_name.get("heartbeat")
                    if detail_tool is not None:
                        input_props = detail_tool.inputSchema.get("properties", {})
                        date_schema = input_props.get("date", {}) if isinstance(input_props, dict) else {}
                        any_of = date_schema.get("anyOf", []) if isinstance(date_schema, dict) else []
                        if {"type": "null"} not in any_of:
                            failures.append("health_detail.date input is not nullable")
                        output = detail_tool.outputSchema or {}
                        for field in ("date", "freshness_seconds", "sleep", "heart_rate"):
                            if not _nullable(output, field):
                                failures.append(f"health_detail.{field} output is not nullable")
                    if heartbeat_tool is not None:
                        output = heartbeat_tool.outputSchema or {}
                        for field in ("bpm", "observed_at", "age_seconds", "freshness_seconds"):
                            if not _nullable(output, field):
                                failures.append(f"heartbeat.{field} output is not nullable")

                    detail = await session.call_tool("health_detail", arguments={})
                    detail_fields = dict(detail.structuredContent) if detail.structuredContent else {}
                    print(f"health_detail -> status={detail_fields.get('status')!r} date={detail_fields.get('date')!r}")
                    if set(detail_fields) != DETAIL_KEYS:
                        failures.append(f"health_detail keys differ from the compact contract: {sorted(detail_fields)}")
                    if detail.isError:
                        failures.append("health_detail returned isError=True")
                    if detail_fields.get("status") == "unavailable":
                        failures.append("health_detail could not reach pulse-relay")

                    beat = await session.call_tool("heartbeat", arguments={})
                    beat_fields = dict(beat.structuredContent) if beat.structuredContent else {}
                    # Presence only: health values never go to the deploy log.
                    print(
                        "heartbeat -> "
                        f"status={beat_fields.get('status')!r} bpm_present={beat_fields.get('bpm') is not None} "
                        f"age_seconds_present={beat_fields.get('age_seconds') is not None}"
                    )
                    if set(beat_fields) != HEARTBEAT_KEYS:
                        failures.append(f"heartbeat keys differ from the compact contract: {sorted(beat_fields)}")
                    if beat.isError:
                        failures.append("heartbeat returned isError=True")
                    if beat_fields.get("status") == "unavailable":
                        failures.append("heartbeat could not reach pulse-relay")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"transport/protocol error: {type(exc).__name__}")  # no message: it can echo tool output

    if failures:
        print("\nFAIL:")
        for item in failures:
            print(f"  - {item}")
        return 1
    print("\nPASS: pulse-relay health MCP discovery, schema, and end-to-end calls OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
