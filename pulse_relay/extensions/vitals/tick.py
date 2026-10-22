"""Persistent, strictly local PulsHealth multi-signal Vitals state machine.

No network APIs, MCP, callbacks or agent delivery. All personal state stays
within a pulse-relay owned 0700 folder; only coarse signal/time and encrypted-by-
filesystem-privacy detector baselines persist. First run silently bootstraps.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import stat
from contextlib import closing
from dataclasses import asdict, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .activity_windows import read_puls_step_buckets
from .signal_windows import read_puls_heart_windows, read_puls_sleep_evidence
from .activity_transitions_core import ActivityState, RestSpan, StepWindow, observe_steps
from .multisignal_core import (
    HeartState, HeartWindow, SleepState, SleepObservation,
    observe_heart, observe_sleep,
)

EVENT_NAME = 'life.vitals.changed'  # ONE future external event, none sent here.
SCHEMA_VERSION = 'vitals-v1'
FRESH_MINUTES = {'activity': 80, 'heart': 30, 'sleep': 90}


def _iso(t: datetime | None) -> str | None:
    if t is None:
        return None
    if not isinstance(t, datetime) or t.tzinfo is None or t.utcoffset() is None:
        raise ValueError('timestamps require timezone information')
    return t.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def _parse(v: str | None) -> datetime | None:
    if v is None:return None
    t=datetime.fromisoformat(v.replace('Z','+00:00'))
    if t.tzinfo is None:raise ValueError('stored timestamp lacks timezone')
    return t.astimezone(timezone.utc)


def _safe_private_db(path: Path) -> None:
    # Avoid traversing a user-supplied symlink or widening state permissions.
    if path.is_symlink():raise ValueError('Vitals state database symlink refused')
    if path.exists():
        s=path.stat()
        if not stat.S_ISREG(s.st_mode) or s.st_uid!=os.getuid():
            raise ValueError('Vitals state database owner/type invalid')
        if stat.S_IMODE(s.st_mode) & 0o077:path.chmod(0o600)


def _prepare_dir(root: Path) -> Path:
    root.mkdir(mode=0o700,parents=True,exist_ok=True)
    s=root.lstat()
    if not stat.S_ISDIR(s.st_mode) or s.st_uid!=os.getuid():
        raise ValueError('Vitals state directory owner/type invalid')
    if stat.S_IMODE(s.st_mode) & 0o077:root.chmod(0o700)
    result=root/'state.sqlite3'
    _safe_private_db(result)
    return result


# Optional fields are written only while set. Once a sleep seed has
# happened, the activity row keeps rest_seed_start/rest_seed_end (they stop
# a session from seeding twice), and versions before these fields cannot
# load it. Before rolling back to such a version, stop the timer and the
# service (a running tick could rewrite them), then run ROLLBACK_SQL on
# state.sqlite3 (pulse_relay/extensions/vitals/README.md).
_OMIT_WHEN_NONE=('rest_seed_start','rest_seed_end','seed_until')
ROLLBACK_SQL=("UPDATE detector_state SET payload=json_remove(payload,"
              "'$.rest_seed_start','$.rest_seed_end','$.seed_until') "
              "WHERE name='activity'")


def _encoded(state: Any) -> str:
    values={k:v for k,v in asdict(state).items()
            if not (k in _OMIT_WHEN_NONE and v is None)}
    return json.dumps({k:_iso(v) if isinstance(v,datetime) else v
                       for k,v in values.items()},sort_keys=True,separators=(',',':'))


def _decode(cls: type, text: str | None) -> Any:
    if not text:return cls()
    values=json.loads(text)
    if not isinstance(values,dict):raise ValueError('invalid Vitals state')
    # Ignore keys this version doesn't know, so state written by a newer
    # version still loads after a rollback.
    known={f.name for f in fields(cls)}
    values={k:v for k,v in values.items() if k in known}
    for field in ('last_end','last_observed_at','rest_seed_start','rest_seed_end','seed_until'):
        if values.get(field) is not None:values[field]=_parse(values[field])
    return cls(**values)


def _state_get(db: sqlite3.Connection,key: str,cls: type) -> Any:
    row=db.execute('SELECT payload FROM detector_state WHERE name=?',(key,)).fetchone()
    return _decode(cls,row[0] if row else None)


def _state_put(db: sqlite3.Connection,key: str,state: Any) -> None:
    db.execute('INSERT INTO detector_state(name,payload) VALUES(?,?) '
               'ON CONFLICT(name) DO UPDATE SET payload=excluded.payload',
               (key,_encoded(state)))


def _fresh(obs: datetime, now: datetime, metric: str) -> bool:
    lag=(now-obs.astimezone(timezone.utc)).total_seconds()/60
    return -2<=lag<=FRESH_MINUTES[metric]


def vitals_tick(data_dir: Path, state_dir: Path, *, now: datetime) -> dict[str, Any]:
    current=_parse(_iso(now))
    assert current is not None
    old_umask=os.umask(0o077)
    try:
        path=_prepare_dir(state_dir)
        with closing(sqlite3.connect(path,timeout=12)) as db:
            db.execute('PRAGMA busy_timeout=12000')
            db.execute('''CREATE TABLE IF NOT EXISTS detector_state(
                          name TEXT PRIMARY KEY,payload TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS metadata(
                          name TEXT PRIMARY KEY,value TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS vitals_candidates(
                          event_id TEXT PRIMARY KEY,
                          event_name TEXT NOT NULL,
                          observed_at TEXT NOT NULL,
                          trigger_names TEXT NOT NULL,
                          recorded_at TEXT NOT NULL)''')
            db.commit()
            _safe_private_db(path)
            db.execute('BEGIN IMMEDIATE')
            row=db.execute("SELECT value FROM metadata WHERE name='schema'").fetchone()
            if row and row[0]!=SCHEMA_VERSION:
                raise ValueError('Vitals schema version mismatch; manual migration required')
            bootstrap=row is None
            if bootstrap:
                db.execute("INSERT INTO metadata(name,value) VALUES('schema',?)",(SCHEMA_VERSION,))
            states={
               'activity':_state_get(db,'activity',ActivityState),
               'heart':_state_get(db,'heart',HeartState),
               'sleep':_state_get(db,'sleep',SleepState),
            }
            changes: list[tuple[str,datetime]]=[]
            # Sleep is read first: a confirmed main session lets the step
            # detector treat an overnight gap as rest instead of unknown.
            sleep=read_puls_sleep_evidence(data_dir,now=current,lookback_hours=36)
            rest=next((RestSpan(row.session_start,row.observed_at) for row in reversed(sleep)
                       if row.state=='asleep' and row.main_session_evidence
                       and row.session_start is not None),None)
            activity=read_puls_step_buckets(data_dir,now=current,lookback_hours=3)
            for row in activity:
                before=states['activity']
                window=StepWindow(row.start,row.end,row.steps,row.received_at,
                                  source=row.source,device_filter=row.device_filter,
                                  complete=row.complete)
                # Source receipt time is used for latency eligibility, while
                # overall freshness is checked separately against current time.
                states['activity'],change=observe_steps(before,window,
                                                         now=max(row.end,row.received_at),
                                                         rest=rest)
                if change and _fresh(change.observed_at,current,'activity'):
                    changes.append((change.reason,change.observed_at))
            heart=read_puls_heart_windows(data_dir,now=current,window_minutes=15,lookback_hours=3)
            for row in heart:
                before=states['heart']
                window=HeartWindow(row.start,row.end,row.median_bpm,row.samples,
                                   row.received_at,source=row.source)
                states['heart'],change=observe_heart(before,window,
                                                    now=max(row.end,row.received_at))
                if change and _fresh(change.observed_at,current,'heart'):
                    changes.append((change.signal,change.observed_at))
            for row in sleep:
                before=states['sleep']
                item=SleepObservation(row.observed_at,row.received_at,row.state,
                                      row.confirmed_minutes,source=row.source,
                                      main_session_evidence=row.main_session_evidence)
                states['sleep'],change=observe_sleep(before,item,
                                                     now=max(row.observed_at,row.received_at))
                if change and _fresh(change.observed_at,current,'sleep'):
                    changes.append((change.signal,change.observed_at))
            for kind,state in states.items():_state_put(db,kind,state)
            accepted=0
            # All sensor channels produce ONE event type. Group near-time
            # transitions from the same poll into bounded batches.
            ordered=sorted(set((name,_iso(when)) for name,when in changes),
                           key=lambda p:(p[1],p[0]))
            groups: list[list[tuple[str,str]]]=[]
            for item in ordered:
                if not groups or _parse(item[1])-_parse(groups[-1][0][1])>timedelta(minutes=10):
                    groups.append([item])
                else:groups[-1].append(item)
            for group in groups:
                if bootstrap:continue  # Historical replay NEVER produces alerts.
                identity=SCHEMA_VERSION+'|'+json.dumps(group,separators=(',',':'))
                eid='vit_'+hashlib.sha256(identity.encode()).hexdigest()[:48]
                names=sorted(set(name for name,_ in group))
                observed=max(when for _,when in group)
                cursor=db.execute('''INSERT OR IGNORE INTO vitals_candidates(
                       event_id,event_name,observed_at,trigger_names,recorded_at)
                       VALUES(?,?,?,?,?)''',
                       (eid,EVENT_NAME,observed,json.dumps(names),_iso(current)))
                accepted+=cursor.rowcount
            # 30-day retention for coarse local-only candidates; no long
            # accumulation of personal context and no raw measurements kept.
            cutoff=_iso(current-timedelta(days=30))
            db.execute('DELETE FROM vitals_candidates WHERE recorded_at < ?', (cutoff,))
            db.execute("""INSERT INTO metadata(name,value) VALUES('last_tick_at',?)
                          ON CONFLICT(name) DO UPDATE SET value=excluded.value""",(_iso(current),))
            db.commit()
            count=db.execute('SELECT COUNT(*) FROM vitals_candidates').fetchone()[0]
        return {
          'source':'pulshealth_only', 'mode':'persistent_no_delivery',
          'bootstrap':bootstrap,
          'activity_windows_3h':len(activity),'heart_windows_3h':len(heart),
          'sleep_evidence_36h':len(sleep),
          'new_vitals_candidates':accepted,'saved_vitals_candidates':count,
          'candidate_event_name':EVENT_NAME,
          'events_sent':0,'subscriptions_created':0,
          'location_subscription_modified':False,
        }
    finally:
        os.umask(old_umask)


def main() -> int:
    parser=argparse.ArgumentParser(description='Persistent Puls-only Vitals; NO notifications')
    parser.add_argument('--data-dir',type=Path,default=Path('/var/lib/pulse-relay'))
    parser.add_argument('--state-dir',type=Path,default=Path('/var/lib/pulse-relay/vitals'))
    args=parser.parse_args()
    try:
        result=vitals_tick(args.data_dir,args.state_dir,now=datetime.now(timezone.utc))
    except sqlite3.Error as exc:
        # Fail the unit so systemd shows it; the transaction was rolled back,
        # so detector state is unchanged. Only the error class is logged.
        print(json.dumps({'error':'store_unreadable','type':type(exc).__name__},
                         sort_keys=True))
        return 1
    print(json.dumps(result,sort_keys=True))
    return 0

if __name__=='__main__':raise SystemExit(main())
