# Schema v1
SQLAlchemy metadata creates users, audits, datasets, plans, input_snapshots, traces. Natural names are unique; foreign keys reference parent entities. Each trace stores an immutable JSON input snapshot.

## Shared input snapshots (v1.1)
`input_snapshots` stores one immutable input copy per `(dataset_id, digest)`; `digest` is the SHA-256 of canonical JSON of `{sensors, readings}` in source order, so row order and duplicate rows participate in identity. `traces.snapshot_id` references the shared row; `traces.snapshot` keeps the per-execution dataset metadata and the plan copy. `traces.request_id` (unique, nullable) makes acceptance idempotent on retry.

Each dataset may hold at most 1 MiB of shared snapshots (`SNAPSHOT_LIMIT_BYTES`, default 1048576). An identical input is always reusable, even at the limit; a new input beyond the limit is rejected with 413 and no partial archive. Acceptance and archive removal lock the dataset row (`SELECT … FOR UPDATE`) so snapshot lookup, quota accounting and reference counts stay consistent; quota is 0 by `traces.snapshot_id` reference count and a shared snapshot is deleted only when its last reference is removed. Traces created before this feature keep their inline `snapshot` and have `snapshot_id = NULL`; they stay viewable and removable. `src/config/schema.py` adds the new columns to existing databases at startup.

PostgreSQL persists metadata and teaching datasets; the bounded Python operators execute the plan.
