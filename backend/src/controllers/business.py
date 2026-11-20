from src.services import datasets,plans,traces
from src.services.audit import log
def save_dataset(data,user,item_id=None):
 row=datasets.save(data,item_id);log(user.email,'dataset.save',row['name']);return row
def save_plan(data,user,item_id=None):
 row=plans.save(data,item_id);log(user.email,'plan.save',row['name']);return row
def start_trace(data,user):
 row=traces.create(data)
 if not row.get('replayed'):log(user.email,'trace.queue',row['name'])
 return row
def remove_trace(item_id,user):
 row=traces.remove(item_id);log(user.email,'trace.remove',f"{item_id}; released={row['released']}; bytes={row['released_bytes']}");return row
