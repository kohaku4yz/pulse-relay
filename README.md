# Pulse Relay

> A small Python server that takes Apple Watch health data, stores it in one SQLite file, and passes short summaries to the AIs.

📖 **Setup guide (中文 / English):** https://kohaku4yz.github.io/pulse-relay/

```
Apple Watch / iPhone (HealthKit)
  → PulsHealth app ── HTTPS · bearer token · gzip NDJSON (Puls Sync v1)
  → Caddy (only /api/health-sync/* is public)
  → server.py 127.0.0.1:8768 → health.sqlite3
  → mcp_server.py :8879 (Claude Code) · health tools in haru-vps-mcp (ChatGPT)
```

- **Server:** Python standard library only, about 2k lines. One SQLite file.
- **Phone side:** the free, open-source [PulsHealth](https://github.com/PulsHealth/pulshealth) app.
- **AI side:** a 2-tool MCP server (`health_detail`, `heartbeat`) with compact, allow-listed output.

## Quick start

Needs a Linux host with systemd, Python ≥ 3.11 with `venv` (`python3-venv` on Debian/Ubuntu), `curl` and `ss` (iproute2). `qrencode` is optional, for the pairing QR code.

```bash
# 1. Install the server (root-owned clone; tests run before the swap, rollback on failure)
sudo git clone https://github.com/kohaku4yz/pulse-relay /root/pulse-relay
sudo bash /root/pulse-relay/deploy/install.sh
sudo bash /opt/pulse-relay/deploy/verify.sh

# 2. Route only the ingest paths through HTTPS: see deploy/caddy-route.example

# 3. Pair the iPhone (prints a puls://pair link and, with qrencode, a QR code)
sudo bash /opt/pulse-relay/deploy/pairing.sh https://health.example.com

# 4. Optional: the MCP server for Claude Code
sudo bash /root/pulse-relay/deploy/install-mcp.sh
claude mcp add --transport http health http://127.0.0.1:8879/mcp
```

The first install creates the ingest token in `/etc/pulse-relay/service.env` (`PULSE_RELAY_INGEST_TOKEN`, root-only). Re-installing never changes it. The pairing link contains the token, so don't share or screenshot it.

In PulsHealth, share Steps, Walking + Running Distance, Flights Climbed, Active Energy, Exercise Minutes, Heart Rate, Resting Heart Rate, HRV, Respiratory Rate, Blood Oxygen, Sleeping Wrist Temperature and Sleep Stages. Add `Sum · All Devices` aggregates for the activity types (daily, plus hourly with a 15-minute settle delay) so Watch and iPhone steps are never double counted.

## API

Public, through the reverse proxy (bearer token):

| Route | |
|---|---|
| `GET /api/health-sync/v1/capabilities` | token check; advertises Puls Sync Protocol v1 |
| `POST /api/health-sync/v1/batches` | protocol-v1 gzip NDJSON ingest |

Loopback only (`127.0.0.1:8768`, no auth, `Cache-Control: no-store`):

| Route | |
|---|---|
| `GET /healthz` | liveness |
| `GET /v1/health/day[?date=YYYY-MM-DD]` | one local day: latest vitals, activity totals, sleep session, heart-rate summary |
| `GET /v1/health/heartbeat` | latest heart rate with `observed_at`, `age_seconds`, `freshness_seconds` |
| `GET /v1/health/now` | compact snapshot across days |
| `GET /v1/health/detail?metric=heart_rate&hours=2&limit=120` | bounded recent series (hours 1–48, limit 1–500) |

`age_seconds` is how old the measurement is; `freshness_seconds` is how long since the phone last synced. Empty or `unavailable` is not zero.

## How data is kept clean

- Only 12 HealthKit types are stored. Other valid Puls lines are accepted and dropped.
- A replayed batch ID writes nothing; a known sample UUID is skipped; HealthKit deletions remove their rows.
- Device names are canonicalized (`Alex's Apple Watch` → `Apple Watch`); rows are keyed by `(metric, observed_at, source)`.
- Activity per day prefers the all-device daily sum, then hourly sums plus a raw tail, then one source's raw rows. Watch and iPhone are never summed.
- Units are normalized at read time (m→km, kJ→kcal, s→min) without rewriting stored rows.
- Sleep comes from HealthKit sleep segments, grouped into the best overnight session.
- Rows from the older Health Auto Export format are still readable (`POST /api/health-ingest` answers 410).

## Configuration

| Variable | Default |
|---|---|
| `PULSE_RELAY_INGEST_TOKEN` | required, ≥32 chars (created by `install.sh`) |
| `PULSE_RELAY_HOST` / `PULSE_RELAY_PORT` | `127.0.0.1` / `8768` (loopback only) |
| `PULSE_RELAY_DATA_DIR` | `/var/lib/pulse-relay` |
| `PULSE_RELAY_MAX_BODY_BYTES` | 5 MiB |
| `PULSE_RELAY_MCP_HOST` / `_PORT` / `_PATH` | `127.0.0.1` / `8879` / `/mcp` |
| `PULSE_RELAY_SERVICE_URL` | `http://127.0.0.1:8768` |

## Layout

```
pulse_relay/
  server.py         stdlib HTTP server: Puls ingest + loopback read routes
  store.py          SQLite schema, Puls parser, dedup, read algorithms
  health_client.py  compact client = the agent contract
  mcp_server.py     2-tool MCP server (FastMCP, streamable HTTP, loopback)
  extensions/       (not included) add your own packages here to mount extra GET routes
deploy/             install / verify / pairing scripts, hardened systemd units, Caddy example
tests/              stdlib unittest
tests_mcp/          MCP tests (need mcp + httpx)
```

## Security notes

- ⚠️ The read routes have no token. Anyone with a shell on the same host can read the data, so run it on a single-user server.
- Only `/api/health-sync/*` should be reachable from the internet. The server refuses to bind anything but loopback.
- iOS decides when PulsHealth syncs, and HealthKit is unreadable while the iPhone is locked, so overnight data arrives after the first unlock.

## Development

```bash
PYTHONPATH=. python3 -m unittest discover -s tests -v
python3 -m pip install "mcp==1.28.1" "httpx==0.28.1"
PYTHONPATH=. python3 -m unittest discover -s tests_mcp -v
```

## License

MIT © kohaku4yz. See [THIRD-PARTY.md](THIRD-PARTY.md).
