# Vitals extension: operations

Optional add-on. The core relay runs without this folder. With it, a
five-minute systemd timer turns PulsHealth data into coarse "something
changed" candidates (`movement_started`, `heart_rate_rose`, `woke_up`, …),
and the server mounts two loopback-only GET routes. How the detectors decide
is in [`ALGORITHM.md`](ALGORITHM.md).

It is **not** a medical monitor. The thresholds are experimental
wellness/activity signals, not clinical diagnoses.

## What it does

- `tick` runs as the `pulse-relay` user every five minutes. It reads
  PulsHealth heart-rate, step-aggregate and sleep-session evidence from
  `health.sqlite3` and nothing else.
- The **first run is a silent bootstrap**: it learns the current state and
  records no historical candidates. Later runs record only fresh, confirmed
  changes. Changes confirmed within ten minutes in one run become one
  candidate.
- The only event name is `life.vitals.changed`.
- It **sends nothing**. A consumer polls the outbox:

      GET http://127.0.0.1:8768/v1/health/vitals-outbox?limit=1..100

  It returns opaque candidate IDs, trigger names, observed/recorded times
  (at most 90 minutes old) and a small context snapshot. It has no detector
  state, raw samples, HealthKit IDs or device names.
- `GET /v1/health/vitals?limit=1..100` (default 30) is the operator's
  status view: last tick, detector state and recent candidates.

Both routes are loopback-only like the rest of the server. Don't add them
to a public reverse-proxy route.

## State

`/var/lib/pulse-relay/vitals/state.sqlite3`: directory 0700, database
0600, owner `pulse-relay`. It holds detector modes, the last processed
windows, a private heart-rate baseline and coarse candidates (trigger
names and timestamps). No raw heart history, individual steps, GPS or
HealthKit UUIDs. Candidates expire after 30 days.

Detector states and new candidates commit in one SQLite transaction, so
replays never duplicate. A schema version mismatch fails closed. Backfilled
or stale samples never create live candidates. Missing signals count as
unknown, not as inactivity.

## Enable, check, disable (root)

Install the core first with `deploy/install.sh`; it deploys `extensions/`
with it. Then:

    bash /opt/pulse-relay/deploy/extensions/vitals/enable-vitals.sh --install
    bash /opt/pulse-relay/deploy/extensions/vitals/enable-vitals.sh --status
    systemctl list-timers --all pulse-relay-vitals.timer

Disable the timer (health data and Vitals state are kept):

    bash /opt/pulse-relay/deploy/extensions/vitals/enable-vitals.sh --disable

The installer copies the units from the installed, tested copy in
`/opt/pulse-relay`, never from a working tree.

## Downgrading past the sleep-seed fields

The `activity` row can contain `rest_seed_start`, `rest_seed_end` and
`seed_until`, which stop one sleep session from seeding twice. A version
without the sleep-seed rule can't load them. Before installing one, as root:

    set -e
    # Stop the timer AND any running tick: both stops block until no tick
    # runs. An interrupted tick leaves the service "failed" and its
    # transaction rolled back, which is fine.
    systemctl stop pulse-relay-vitals.timer
    systemctl stop pulse-relay-vitals.service
    sudo -u pulse-relay python3 -I - <<'PY'
    import sqlite3
    from contextlib import closing
    with closing(sqlite3.connect("/var/lib/pulse-relay/vitals/state.sqlite3")) as db:
        db.execute("UPDATE detector_state SET payload=json_remove(payload,'$.rest_seed_start','$.rest_seed_end','$.seed_until') WHERE name='activity'")
        db.commit()
    PY

Then install the older version and start the timer again. The SQL is
`ROLLBACK_SQL` in `tick.py`; a test keeps this page in sync with it.

## Guardrails

- The timer's oneshot is separate from `pulse-relay.service`.
- systemd blocks all network (`IPAddressDeny=any`, AF_UNIX only). It also
  sets a private /tmp, UMask 0077, NoNewPrivileges and a read-only system.
- `health.sqlite3` is bind-mounted read-only. The data directory itself is
  writable because the DB is in WAL mode: even a read-only SQLite reader
  must be able to create the `-shm`/`-wal` files next to it.
- A store that can't be read fails the run (`store_unreadable`, exit 1)
  instead of counting as "no data".
- journald gets only aggregate counts and status, no health values or exact
  observation times.
- PulsHealth background uploads can lag (HealthKit is locked while the
  phone is locked), so overnight changes often arrive too late to count.
