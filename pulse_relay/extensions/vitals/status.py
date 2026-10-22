"""Read the owner's existing private Vitals database, READ ONLY.

Called only by pulse-relay's loopback-only HTTP server, as pulse-relay.
No new filesystem permission grants, files, daemon, webhook or MCP task.
"""
from __future__ import annotations

import json
import sqlite3
import stat
from pathlib import Path
from typing import Any

EVENT_NAME = 'life.vitals.changed'
EXPECTED_SCHEMA = 'vitals-v1'
ALLOWED_SIGNALS = frozenset({
    'movement_started','movement_settled','heart_rate_rose',
    'heart_rate_fell','sleep_started','woke_up',
})
DETECTOR_FIELDS = {
    'activity': frozenset({'mode','pending','pending_count','last_end'}),
    'heart': frozenset({'anchor_bpm','pending','pending_count','last_end'}),
    'sleep': frozenset({'mode','last_observed_at'}),
}


def read_vitals_status(data_dir: Path, *, limit: int = 30) -> dict[str, Any]:
    """Return bounded full detector states + confirmed candidates.

    No direct health metric_samples query; detector baseline is available
    because the owner explicitly authorized the existing Health channel.
    """
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('limit must be an integer from 1 to 100')
    path = data_dir / 'vitals' / 'state.sqlite3'
    base = {
        'mode': 'persistent_no_delivery',
        'candidate_event_name': EVENT_NAME,
        'detectors': {}, 'recent_candidates': [], 'total_candidates': 0,
        'last_run_at': None, 'events_sent': 0,
        'subscriptions_created': 0, 'location_subscription_modified': False,
    }
    try:
        if path.is_symlink() or not path.is_file() or not stat.S_ISREG(path.stat().st_mode):
            return {'status':'unavailable','reason':'vitals_not_initialized',**base}
        with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True,timeout=2) as db:
            db.row_factory=sqlite3.Row
            db.execute('PRAGMA query_only=ON')
            info=dict(db.execute("SELECT name,value FROM metadata WHERE name IN ('schema','last_tick_at')"))
            if info.get('schema')!=EXPECTED_SCHEMA:
                return {'status':'unavailable','reason':'unknown_vitals_schema',**base}
            detectors={}
            for row in db.execute('SELECT name,payload FROM detector_state'):
                name=row['name']
                if name not in DETECTOR_FIELDS:continue
                parsed=json.loads(row['payload'])
                if not isinstance(parsed,dict):raise ValueError('invalid detector state')
                detectors[name]={k:v for k,v in parsed.items() if k in DETECTOR_FIELDS[name]}
            total=int(db.execute('SELECT COUNT(*) FROM vitals_candidates').fetchone()[0])
            recent=[]
            for row in db.execute('''
              SELECT event_name, observed_at, trigger_names, recorded_at
              FROM vitals_candidates ORDER BY recorded_at DESC,observed_at DESC LIMIT ?
            ''',(limit,)):
                if row['event_name']!=EVENT_NAME:continue
                triggers=json.loads(row['trigger_names'])
                if not (isinstance(triggers,list) and triggers and all(
                    isinstance(item,str) and item in ALLOWED_SIGNALS for item in triggers
                )):continue
                recent.append({
                    'event_name':EVENT_NAME, 'triggers':triggers,
                    'observed_at':row['observed_at'],
                    'recorded_at':row['recorded_at'],
                })
        return {
            'status':'ok', 'schema':EXPECTED_SCHEMA, **base,
            'detectors':detectors,'total_candidates':total,
            'recent_candidates':recent,'last_run_at':info.get('last_tick_at'),
        }
    except (sqlite3.Error,ValueError,TypeError,OSError,json.JSONDecodeError):
        return {'status':'unavailable','reason':'vitals_state_unreadable',**base}
