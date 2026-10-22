import unittest, tempfile, sqlite3, json, uuid
from datetime import datetime,timezone,timedelta
from pathlib import Path
from pulse_relay.store import initialize_store
from pulse_relay.extensions.vitals.context_snapshot import read_puls_context

NOW=datetime(2026,10,12,9,0,tzinfo=timezone.utc)
def z(dt):return dt.isoformat().replace('+00:00','Z')

def put(db,metric,at,value,unit,*,proven=True,source='Apple Watch'):
    stamp=z(at)
    db.execute('''INSERT INTO metric_samples(metric, observed_at, local_date, source, units, kind, payload_json, received_at)
                  VALUES(?,?,?,?,?,?,?,?)''',
               (metric,stamp,'2026-10-12',source,unit,'quantity',json.dumps({'qty':value}),z(NOW)))
    if proven:
        db.execute('INSERT INTO puls_sample_map(uuid,metric,observed_at,source) VALUES(?,?,?,?)',
                   (str(uuid.uuid5(uuid.NAMESPACE_URL,metric+stamp+source)),metric,stamp,source))

class ContextTests(unittest.TestCase):
    def test_recent_heart_from_puls_not_hae(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root);initialize_store(path)
            with sqlite3.connect(path/'health.sqlite3') as db:
                put(db,'heart_rate',NOW-timedelta(minutes=5),80,'count/min',proven=True)
                put(db,'heart_rate',NOW-timedelta(minutes=4),200,'count/min',proven=False)
                put(db,'heart_rate_variability',NOW-timedelta(hours=2),45,'ms')
                put(db,'resting_heart_rate',NOW-timedelta(hours=1),62,'count/min')
            snapshot=read_puls_context(path,now=NOW)
            self.assertEqual(snapshot['heart_rate_bpm'].value,80)
            self.assertEqual(snapshot['hrv_ms'].value,45)
            self.assertEqual(snapshot['resting_heart_rate_bpm'].value,62)
            self.assertNotIn('steps_last_hour',snapshot)
            self.assertNotIn('sleep_state',snapshot)

    def test_no_hae_and_no_empty_spurious_zero(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root);initialize_store(path)
            with sqlite3.connect(path/'health.sqlite3') as db:
                put(db,'heart_rate',NOW-timedelta(minutes=5),100,'count/min',proven=False)
            self.assertEqual(read_puls_context(path,now=NOW),{})

    def test_steps_label_requires_substantial_completed_coverage(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root);initialize_store(path)
            with sqlite3.connect(path/'health.sqlite3') as db:
                for i in range(12):
                    a=NOW-timedelta(minutes=60-i*5);end=a+timedelta(minutes=5)
                    db.execute('''INSERT INTO metric_samples(metric, observed_at,local_date,source,units,kind,payload_json,received_at)
                                  VALUES(?,?,?,?,?,?,?,?)''',
                        ('step_count',z(a),'2026-10-12','PulsHealth aggregate:all:5:minute','count','aggregate',
                         json.dumps({'qty':20,'func':'sum','interval_value':5,'interval_unit':'minute',
                                     'device_filter':'all','bucket_end':z(end)}),z(NOW)))
            snapshot=read_puls_context(path,now=NOW)
            self.assertEqual(snapshot['steps_last_hour'].value,240)

if __name__=='__main__':unittest.main()
