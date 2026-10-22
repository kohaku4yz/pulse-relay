"""Read-only, source-verified PulsHealth Vitals handoff for localhost producer.

No events are sent, acknowledged, deleted, or published here. The Events
consumer must independently validate every field and explicitly opt in.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .context_snapshot import read_puls_context

_NAME = 'life.vitals.changed'
_ID = re.compile(r'^vit_[0-9a-f]{48}$')
_SIGNALS = frozenset(('movement_started','movement_settled','heart_rate_rose',
                     'heart_rate_fell','sleep_started','woke_up'))


def _instant(value: str) -> datetime:
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError('source event must have timezone')
    return dt.astimezone(timezone.utc)


def _stamp(dt: datetime) -> str:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError('now must be timezone-aware')
    return dt.astimezone(timezone.utc).isoformat().replace('+00:00','Z')


def read_vitals_outbox(data_dir: Path, *, now: datetime,
                       limit: int = 50) -> dict[str, Any]:
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('invalid candidate limit')
    generated=_stamp(now)
    result={'status':'ok','source':'pulshealth_only',
            'generated_at':generated,'event_name':_NAME,
            'candidates':[],'snapshot':{}}
    path=data_dir/'vitals'/'state.sqlite3'
    if path.is_symlink() or not path.is_file():
        return {**result,'status':'unavailable'}
    try:
        with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True,timeout=2) as db:
            db.row_factory=sqlite3.Row
            db.execute('PRAGMA query_only=ON')
            version=db.execute("SELECT value FROM metadata WHERE name='schema'").fetchone()
            if not version or version['value']!='vitals-v1':
                return {**result,'status':'unavailable'}
            items=[]
            for row in db.execute('''SELECT event_id,event_name,observed_at,
                     trigger_names,recorded_at FROM vitals_candidates
                     ORDER BY recorded_at DESC,observed_at DESC LIMIT ?''',(limit,)):
                if row['event_name']!=_NAME or not _ID.fullmatch(row['event_id']):
                    continue
                names=json.loads(row['trigger_names'])
                if not (isinstance(names,list) and 1<=len(names)<=6
                    and len(set(names))==len(names)
                    and all(isinstance(n,str) and n in _SIGNALS for n in names)):
                    continue
                observed=_instant(row['observed_at'])
                recorded=_instant(row['recorded_at'])
                current=now.astimezone(timezone.utc)
                # Stale changes are never handed out: the full
                # historical candidate list remains readable from the
                # separate /vitals diagnostics endpoint instead.
                if (observed>current or recorded>current or
                    current-observed>timedelta(minutes=90)):
                    continue
                items.append({'event_id':row['event_id'],'triggers':names,
                              'observed_at':_stamp(observed),
                              'recorded_at':_stamp(recorded)})
            result['candidates']=items
    except (sqlite3.Error, ValueError, TypeError, OSError):
        return {**result,'status':'unavailable'}

    # Source worker polls every minute, but live health context is needed
    # ONLY when a confirmed transition exists. Avoid scanning recent heart,
    # step and sleep history on thousands of empty source polls per day.
    # A later candidate always receives fresh context at poll time.
    if not result['candidates']:
        return result

    try:
        readings=read_puls_context(data_dir,now=now)
    except (sqlite3.Error, OSError, ValueError):
        readings={}
    result['snapshot']={key:{'value':item.value,
                             'observed_at':_stamp(item.observed_at),
                             'source':'pulshealth'}
                        for key,item in readings.items()}
    return result
