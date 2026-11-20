import time,logging,redis
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.trace import Trace
from src.services import snapshots
from src.services.operators import execute
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
while True:
 Path('/tmp/worker-heartbeat').touch()
 try:
  item=q.blpop('operator-traces',timeout=2)
  if not item:continue
  run_id=int(item[1])
  with Session() as s:
   row=s.get(Trace,run_id)
   if not row or row.status!='queued':continue
   row.status='running';s.commit()
   try:dataset,plan=snapshots.materialize(s,row)
   except Exception as exc:
    # The archived input could not be reassembled; record the failure instead of
    # leaving the task stuck in running.
    logging.exception('snapshot unavailable')
    s.rollback();r=s.get(Trace,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit();continue
  try:
   result=execute(dataset,plan['steps'])
   with Session() as s:r=s.get(Trace,run_id);r.result=result;r.status='completed';s.commit()
   log('worker','trace.completed',str(run_id))
  except Exception as exc:
   logging.exception('trace failed')
   with Session() as s:r=s.get(Trace,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit()
 except Exception:logging.exception('worker loop');time.sleep(2)
