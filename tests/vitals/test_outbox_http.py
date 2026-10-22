"""Real local HTTP Vitals candidate reader, fixture-only and no delivery."""
from __future__ import annotations
import http.client,json,sqlite3,tempfile,threading,unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
from pulse_relay.store import initialize_store
from pulse_relay.server import ServiceSettings,build_http_server

class OutboxHttpTests(unittest.TestCase):
 def test_loopback_outbox_only_and_no_hae(self):
  with tempfile.TemporaryDirectory() as folder:
   data=Path(folder)
   initialize_store(data)
   state=data/'vitals';state.mkdir(mode=0o700)
   with sqlite3.connect(state/'state.sqlite3') as db:
    db.execute('CREATE TABLE metadata(name TEXT PRIMARY KEY,value TEXT NOT NULL)')
    db.execute('INSERT INTO metadata VALUES (?,?)',('schema','vitals-v1'))
    db.execute('CREATE TABLE vitals_candidates(event_id TEXT PRIMARY KEY,event_name TEXT NOT NULL,observed_at TEXT NOT NULL,trigger_names TEXT NOT NULL,recorded_at TEXT NOT NULL)')
    db.execute('INSERT INTO vitals_candidates VALUES(?,?,?,?,?)',('vit_'+'f'*48,'life.vitals.changed',(datetime.now(timezone.utc)-timedelta(minutes=5)).isoformat(),json.dumps(['heart_rate_rose']),(datetime.now(timezone.utc)-timedelta(minutes=4)).isoformat()))
   settings=ServiceSettings('127.0.0.1',0,data,'x'*32,5242880)
   srv=build_http_server(settings)
   thread=threading.Thread(target=srv.serve_forever,daemon=True);thread.start()
   try:
    with patch('pulse_relay.extensions.vitals.outbox.read_puls_context',return_value={}):
     c=http.client.HTTPConnection('127.0.0.1',srv.server_address[1],timeout=3)
     c.request('GET','/v1/health/vitals-outbox?limit=1')
     answer=c.getresponse();self.assertEqual(answer.status,200)
     self.assertEqual(answer.getheader('Cache-Control'),'no-store')
     payload=json.loads(answer.read())
     self.assertEqual(payload['candidates'][0]['triggers'],['heart_rate_rose'])
     self.assertEqual(payload['source'],'pulshealth_only')
     c.request('GET','/v1/health/vitals-outbox?limit=999')
     self.assertEqual(c.getresponse().status,400);c.close()
   finally:
    srv.shutdown();srv.server_close();thread.join(timeout=3)
if __name__=='__main__':unittest.main()
