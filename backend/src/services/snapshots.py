import hashlib,json
from sqlalchemy import func
from fastapi import HTTPException
from src.config.settings import SNAPSHOT_LIMIT_BYTES
from src.models.snapshot import InputSnapshot
from src.models.trace import Trace

def canonical(payload):
 return json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')

def input_payload(dataset):
 """Copy table data in source order; row order and duplicate rows are part of the snapshot identity."""
 return {'sensors':[dict(r) for r in dataset.sensors],'readings':[dict(r) for r in dataset.readings]}

def fingerprint(payload):
 blob=canonical(payload)
 return hashlib.sha256(blob).hexdigest(),len(blob)

def snapshot_limit():
 return SNAPSHOT_LIMIT_BYTES

def usage(s,dataset_id):
 return int(s.query(func.coalesce(func.sum(InputSnapshot.size_bytes),0)).filter(InputSnapshot.dataset_id==dataset_id).scalar() or 0)

def acquire(s,dataset_id,payload):
 """Reuse the dataset's identical snapshot, or store a new one within the 1 MiB quota.

 The caller holds the dataset row lock, so digest lookup, quota accounting and
 insertion are serialized against concurrent acceptance and archive removal.
 An identical input is always reusable, even when the dataset quota is full.
 """
 digest,size=fingerprint(payload)
 existing=s.query(InputSnapshot).filter_by(dataset_id=dataset_id,digest=digest).first()
 if existing:return existing,True
 used=usage(s,dataset_id)
 if used+size>SNAPSHOT_LIMIT_BYTES:
  raise HTTPException(413,f'该数据集共享输入快照已占用 {used} / {SNAPSHOT_LIMIT_BYTES} 字节；相同输入仍可执行，新输入超出容量请先移除已结束的存档。')
 row=InputSnapshot(dataset_id=dataset_id,digest=digest,size_bytes=size,payload=payload)
 s.add(row);s.flush()
 return row,False

def refcount(s,snapshot_id):
 if not snapshot_id:return 0
 return int(s.query(func.count(Trace.id)).filter(Trace.snapshot_id==snapshot_id).scalar() or 0)

def release_if_unused(s,snapshot_id):
 """Drop the shared snapshot only after the last trace reference is gone."""
 if not snapshot_id:return False,0
 if refcount(s,snapshot_id):return False,0
 row=s.get(InputSnapshot,snapshot_id)
 if not row:return False,0
 size=row.size_bytes;s.delete(row);s.flush()
 return True,size

def materialize(s,row):
 """Reassemble the immutable input recorded when the trace was accepted."""
 snap=dict(row.snapshot or {})
 dataset=dict(snap.get('dataset') or {})
 if row.snapshot_id:
  shared=s.get(InputSnapshot,row.snapshot_id)
  if not shared:raise HTTPException(410,'共享输入快照已不可用')
  dataset['sensors']=shared.payload.get('sensors',[])
  dataset['readings']=shared.payload.get('readings',[])
 return dataset,dict(snap.get('plan') or {})

def annotate_list(s,records,rows):
 ids={row.snapshot_id for row in rows if row.snapshot_id}
 refs=dict(s.query(Trace.snapshot_id,func.count(Trace.id)).filter(Trace.snapshot_id.in_(ids)).group_by(Trace.snapshot_id).all()) if ids else {}
 sizes=dict(s.query(InputSnapshot.id,InputSnapshot.size_bytes).filter(InputSnapshot.id.in_(ids)).all()) if ids else {}
 dataset_ids={row.dataset_id for row in rows}
 used={dataset_id:usage(s,dataset_id) for dataset_id in dataset_ids}
 for rec,row in zip(records,rows):
  if row.snapshot_id:
   rec['snapshot_refs']=refs.get(row.snapshot_id,0)
   rec['snapshot_bytes']=sizes.get(row.snapshot_id,0)
  rec['snapshot_usage']={'dataset_id':row.dataset_id,'used_bytes':used.get(row.dataset_id,0),'limit_bytes':SNAPSHOT_LIMIT_BYTES}
 return records

def annotate_detail(s,rec,row):
 if row.snapshot_id:
  shared=s.get(InputSnapshot,row.snapshot_id)
  if shared:
   rec['snapshot_refs']=refcount(s,row.snapshot_id)
   rec['snapshot_bytes']=shared.size_bytes
   base=dict(rec.get('snapshot') or {})
   dataset=dict(base.get('dataset') or {})
   dataset['sensors']=shared.payload.get('sensors',[])
   dataset['readings']=shared.payload.get('readings',[])
   base['dataset']=dataset
   rec['snapshot']=base
 rec['snapshot_usage']={'dataset_id':row.dataset_id,'used_bytes':usage(s,row.dataset_id),'limit_bytes':SNAPSHOT_LIMIT_BYTES}
 return rec
