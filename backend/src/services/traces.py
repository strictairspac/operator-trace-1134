import time,redis
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.dataset import Dataset
from src.models.snapshot import InputSnapshot
from src.models.plan import Plan
from src.models.trace import Trace
from src.services import snapshots
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
ACCEPT_ATTEMPTS=3

def _replay(s,existing):
 return snapshots.annotate_detail(s,record(existing),existing)|{'replayed':True}

def list_items():
 with Session() as s:
  rows=s.query(Trace).order_by(Trace.id.desc()).limit(100).all()
  return snapshots.annotate_list(s,[record(r) for r in rows],rows)
def detail(item_id):
 with Session() as s:
  row=s.get(Trace,item_id)
  if not row:raise HTTPException(404,'未找到执行轨迹')
  return snapshots.annotate_detail(s,record(row),row)
def inputs(item_id):
 with Session() as s:
  row=s.get(Trace,item_id)
  if not row:raise HTTPException(404,'未找到执行轨迹')
  return snapshots.materialize(s,row)[0]

def _accept_once(s,data):
 """One acceptance attempt inside a transaction; returns (trace, replay) or None if a
 concurrent write made this attempt lose (caller retries against fresh state)."""
 if data.request_id:
  existing=s.query(Trace).filter_by(request_id=data.request_id).first()
  if existing:return _replay(s,existing),True
 p=s.get(Plan,data.plan_id)
 if not p:raise HTTPException(404,'未找到算子计划')
 # Lock the dataset so snapshot reuse, quota accounting and archive removal
 # for one dataset cannot interleave.
 d=s.query(Dataset).filter(Dataset.id==p.dataset_id).with_for_update().one_or_none()
 if not d:raise HTTPException(404,'未找到数据集')
 snap,shared=snapshots.acquire(s,d.id,snapshots.input_payload(d))
 row=Trace(name=data.name,plan_id=p.id,dataset_id=d.id,status='queued',request_id=data.request_id,
  snapshot={'dataset':{'id':d.id,'name':d.name,'description':d.description},'plan':record(p),'snapshot_id':snap.id},
  snapshot_id=snap.id)
 s.add(row)
 s.flush()
 accepted=record(row)
 accepted['snapshot_refs']=snapshots.refcount(s,snap.id)
 accepted['snapshot_bytes']=snap.size_bytes
 accepted['snapshot_shared']=shared
 accepted['snapshot_usage']={'dataset_id':d.id,'used_bytes':snapshots.usage(s,d.id),'limit_bytes':snapshots.snapshot_limit()}
 return accepted,False

def create(data):
 accepted=None
 for attempt in range(ACCEPT_ATTEMPTS):
  try:
   with Session() as s:
    accepted,replay=_accept_once(s,data)
    if replay:
     s.rollback()
     return accepted
    s.commit()
   break
  except IntegrityError:
   # A concurrent acceptance inserted the same shared snapshot or the same
   # request id first. Retrying re-reads both, so quota is never charged twice.
   if attempt==ACCEPT_ATTEMPTS-1:raise
   time.sleep(0.05*(attempt+1))
 try:queue.rpush('operator-traces',accepted['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(Trace,accepted['id']);r.status='failed';r.error='队列不可用，请创建新任务';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 return accepted

def remove(item_id):
 with Session() as s:
  row=s.query(Trace).filter(Trace.id==item_id).with_for_update().one_or_none()
  if not row:raise HTTPException(404,'未找到执行轨迹')
  if row.status not in ('completed','failed'):
   raise HTTPException(409,'只能移除已结束的存档；排队或执行中的任务仍会写入剖面')
  dataset_id=row.dataset_id;snapshot_id=row.snapshot_id
  # Same dataset lock as acceptance keeps reference counts and capacity in sync.
  s.query(Dataset).filter(Dataset.id==dataset_id).with_for_update().one()
  s.delete(row);s.flush()
  released,size=snapshots.release_if_unused(s,snapshot_id)
  remaining=snapshots.refcount(s,snapshot_id)
  used=snapshots.usage(s,dataset_id)
  s.commit()
  return {'id':item_id,'dataset_id':dataset_id,'snapshot_id':snapshot_id,'released':released,
   'released_bytes':size,'snapshot_refs':remaining,'used_bytes':used,'limit_bytes':snapshots.snapshot_limit()}
