"""Puls-only aggregate normalization from local synthetic SQLite fixtures."""
import unittest
from datetime import datetime,timedelta,timezone
import json,sqlite3,tempfile
from pathlib import Path

from pulse_relay.store import initialize_store
from pulse_relay.extensions.vitals.activity_windows import read_puls_step_buckets

NOW=datetime(2026,10,12,8,tzinfo=timezone.utc)

def add(db,minute,*,source='PulsHealth aggregate:all:5:minute',
        window=5,value=60,kind='aggregate',units='count',device_filter='all'):
    start=NOW+timedelta(minutes=minute)
    end=start+timedelta(minutes=window)
    payload={"qty":value,"func":"sum","interval_value":window,
             "interval_unit":"minute","device_filter":device_filter,
             "bucket_end":end.isoformat().replace("+00:00","Z")}
    db.execute(
      """INSERT INTO metric_samples
         (metric,observed_at,local_date,source,units,kind,payload_json,received_at)
         VALUES(?,?,?,?,?,?,?,?)""",
         ("step_count",start.isoformat().replace("+00:00","Z"),
          "2026-10-12",source,units,kind,json.dumps(payload),
          end.isoformat().replace("+00:00","Z")))

class TestPulsActivityWindows(unittest.TestCase):
    def test_only_completed_all_device_puls_buckets(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path)
            initialize_store(root)
            with sqlite3.connect(root/"health.sqlite3") as db:
                add(db,-20,source="PulsHealth aggregate:all:5:minute")
                add(db,-15,source="PulsHealth aggregate:watch:5:minute",device_filter="watch")
                add(db,-10,source="HAE aggregate:all:5:minute")
                add(db,-5,source="PulsHealth aggregate:all:5:minute")
            items=read_puls_step_buckets(root,now=NOW)
            self.assertEqual(len(items),2)
            self.assertTrue(all(x.device_filter=="all" and x.source=="pulshealth" for x in items))
            self.assertEqual([x.steps for x in items],[60.0,60.0])

    def test_no_hae_or_daily_aggregate(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path)
            initialize_store(root)
            with sqlite3.connect(root/"health.sqlite3") as db:
                add(db,-20,source="HAE",value=500)
                add(db,-15,source="PulsHealth aggregate:all:1:day",window=1440,value=1000)
            self.assertEqual(read_puls_step_buckets(root,now=NOW),[])

    def test_missing_database(self):
        with tempfile.TemporaryDirectory() as path:
            self.assertEqual(read_puls_step_buckets(Path(path),now=NOW),[])

    def test_overlapping_buckets_use_smaller(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);initialize_store(root)
            with sqlite3.connect(root/"health.sqlite3") as db:
                add(db,-60,source="PulsHealth aggregate:all:1:hour",window=60,value=500)
                add(db,-15,source="PulsHealth aggregate:all:5:minute",window=5,value=100)
            items=read_puls_step_buckets(root,now=NOW)
            self.assertEqual(len(items),1)
            self.assertEqual(items[0].steps,100)

if __name__=="__main__":
    unittest.main()
