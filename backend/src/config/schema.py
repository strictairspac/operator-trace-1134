from sqlalchemy import inspect,text
from src.config.database import Base,engine
# Register every model so create_all sees complete foreign-key targets.
import src.models.identity,src.models.dataset,src.models.plan,src.models.snapshot,src.models.trace  # noqa: F401

def ensure_schema():
 """Create metadata and add shared-snapshot columns to databases created before this feature."""
 Base.metadata.create_all(engine)
 inspector=inspect(engine)
 if 'traces' not in inspector.get_table_names():return
 columns={c['name'] for c in inspector.get_columns('traces')}
 with engine.begin() as conn:
  if 'snapshot_id' not in columns:
   conn.execute(text('ALTER TABLE traces ADD COLUMN snapshot_id INTEGER REFERENCES input_snapshots(id)'))
   conn.execute(text('CREATE INDEX IF NOT EXISTS ix_traces_snapshot_id ON traces (snapshot_id)'))
  if 'request_id' not in columns:
   conn.execute(text('ALTER TABLE traces ADD COLUMN request_id VARCHAR(120)'))
   conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_traces_request_id ON traces (request_id)'))
