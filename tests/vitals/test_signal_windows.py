"""Generated-only HealthKit fixtures. Never query the owner's production DB."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import json,sqlite3,tempfile,unittest
from pulse_relay.store import initialize_store
from pulse_relay.extensions.vitals.signal_windows import read_puls_heart_windows,read_puls_sleep_evidence

T=datetime(2026,10,12,6,0,tzinfo=timezone.utc)
def utc(t):return t.isoformat().replace('+00:00','Z')
def sample(db,metric,start,payload,*,kind='quantity',source='Apple Watch',units='count/min',proven=True,end=None):
    stamp=utc(start);uuid='e0000000-0000-4000-8000-'+format(int(start.timestamp())%2**36,'012x')
    if metric=='sleep_analysis':uuid='f0000000-0000-4000-8000-'+format(int(start.timestamp())%2**36,'012x')
    db.execute('''INSERT INTO metric_samples(metric,observed_at,local_date,source,units,kind,payload_json,received_at)
                VALUES(?,?,?,?,?,?,?,?)''',(metric,stamp,'2026-10-12',source,units,kind,json.dumps(payload),utc(end or T)))
    if proven:db.execute('''INSERT INTO puls_sample_map(uuid,metric,observed_at,source) VALUES(?,?,?,?)''',(uuid,metric,stamp,source))

class PulsWindowTests(unittest.TestCase):
 def test_heart_window_uses_puls_uuid_not_hae_label(self):
    with tempfile.TemporaryDirectory() as tmp:
     d=Path(tmp);initialize_store(d)
     with sqlite3.connect(d/'health.sqlite3') as c:
      for i,x in enumerate([70,75,80,180]):
       sample(c,'heart_rate',T+timedelta(minutes=2*i),{'qty':x},proven=i<3)
     rows=read_puls_heart_windows(d,now=T+timedelta(minutes=16))
     self.assertEqual(len(rows),1)
     self.assertEqual(rows[0].median_bpm,75)
     self.assertEqual(rows[0].samples,3)

 def test_sparse_heart_data_safely_returns_no_windows(self):
    with tempfile.TemporaryDirectory() as tmp:
     d=Path(tmp);initialize_store(d)
     with sqlite3.connect(d/'health.sqlite3') as c:
      sample(c,'heart_rate',T,{'qty':86})
     self.assertEqual(read_puls_heart_windows(d,now=T+timedelta(minutes=15)),[])

 def test_sleep_requires_explicit_sustained_coverage_and_awake(self):
    with tempfile.TemporaryDirectory() as tmp:
     d=Path(tmp);initialize_store(d)
     start=T-timedelta(hours=3)
     with sqlite3.connect(d/'health.sqlite3') as c:
      for n,stage in enumerate(['core','deep','rem']):
       a=start+timedelta(hours=n);b=a+timedelta(hours=1)
       sample(c,'sleep_analysis',a,{'stage':stage,'start':utc(a),'end':utc(b)},kind='sleep_segment',units='category')
      # 5-minute nighttime arousal isn't sufficient; explicit 35m awake is.
      a=T;b=T+timedelta(minutes=35)
      sample(c,'sleep_analysis',a,{'stage':'awake','start':utc(a),'end':utc(b)},kind='sleep_segment',units='category')
     result=read_puls_sleep_evidence(d,now=T+timedelta(hours=1))
     self.assertEqual([x.state for x in result],['asleep','awake'])
     self.assertAlmostEqual(result[0].confirmed_minutes,180)

 def test_no_wake_for_brief_arousal(self):
    with tempfile.TemporaryDirectory() as tmp:
     d=Path(tmp);initialize_store(d);start=T-timedelta(hours=3)
     with sqlite3.connect(d/'health.sqlite3') as c:
      for n in range(3):
       a=start+timedelta(hours=n);b=a+timedelta(hours=1)
       sample(c,'sleep_analysis',a,{'stage':'core','start':utc(a),'end':utc(b)},kind='sleep_segment',units='category')
      a=T;b=a+timedelta(minutes=5)
      sample(c,'sleep_analysis',a,{'stage':'awake','start':utc(a),'end':utc(b)},kind='sleep_segment',units='category')
     result=read_puls_sleep_evidence(d,now=T+timedelta(hours=1))
     self.assertEqual([x.state for x in result],['asleep'])

if __name__=='__main__':unittest.main()
