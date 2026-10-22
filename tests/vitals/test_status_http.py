"""Actual loopback HTTP handler for owner-approved Vitals state."""
from __future__ import annotations
import http.client,json,tempfile,threading,unittest
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
from pulse_relay.server import build_http_server,ServiceSettings
from pulse_relay.extensions.vitals.tick import vitals_tick

T=datetime(2026,10,12,11,tzinfo=timezone.utc)
class VitalsHttpTests(unittest.TestCase):
    def test_http_read_and_invalid_query(self):
        with tempfile.TemporaryDirectory() as directory:
            data=Path(directory)
            with patch('pulse_relay.extensions.vitals.tick.read_puls_step_buckets',return_value=[]),patch('pulse_relay.extensions.vitals.tick.read_puls_heart_windows',return_value=[]),patch('pulse_relay.extensions.vitals.tick.read_puls_sleep_evidence',return_value=[]):
                vitals_tick(data,data/'vitals',now=T)
            settings=ServiceSettings('127.0.0.1',0,data,'z'*32,5242880)
            server=build_http_server(settings)
            thread=threading.Thread(target=server.serve_forever,daemon=True)
            thread.start()
            try:
                for path,expect in (('/v1/health/vitals',200),('/v1/health/vitals?limit=1',200),('/v1/health/vitals?limit=101',400),('/v1/health/vitals?debug=yes',400)):
                    conn=http.client.HTTPConnection('127.0.0.1',server.server_address[1],timeout=3)
                    conn.request('GET',path)
                    response=conn.getresponse()
                    self.assertEqual(response.status,expect)
                    if expect==200:
                        self.assertEqual(response.getheader('Cache-Control'),'no-store')
                        report=json.loads(response.read())
                        self.assertEqual(report['status'],'ok')
                        self.assertEqual(report['last_run_at'],'2026-10-12T11:00:00Z')
                    else:response.read()
                    conn.close()
            finally:
                server.shutdown();server.server_close();thread.join(timeout=3)
if __name__=='__main__':unittest.main()
