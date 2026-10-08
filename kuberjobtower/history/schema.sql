-- Run-history read model, schema v1. Derived from the journal and rebuildable at any time.
-- Times are epoch milliseconds (UTC). No secrets. Foreign keys are declared for documentation and
-- checked by `history verify`, not enforced, because journal chunks can arrive out of order.

CREATE TABLE runs (
  run_uid      TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL,
  aoi          TEXT,
  methodology  TEXT,
  status       TEXT NOT NULL DEFAULT 'running',     -- running | succeeded | failed | aborted (sticky once terminal)
  started_ms   INTEGER,
  finished_ms  INTEGER,
  image        TEXT,
  image_digest TEXT,
  cluster      TEXT,
  namespace    TEXT,
  submitted_by TEXT,
  spec_json    TEXT,
  observed_ms  INTEGER NOT NULL
) STRICT;
CREATE INDEX runs_started ON runs (started_ms DESC);
CREATE INDEX runs_run_id ON runs (run_id);

CREATE TABLE configs (                                -- distinct Job configurations, stored once
  config_sha      TEXT PRIMARY KEY,
  normalized_json TEXT NOT NULL,
  first_seen_ms   INTEGER NOT NULL
) STRICT;

CREATE TABLE jobs (
  job_id           TEXT PRIMARY KEY,                  -- '<run_uid>/<job_name>'
  run_uid          TEXT NOT NULL REFERENCES runs (run_uid),
  job_name         TEXT NOT NULL,
  phase            TEXT NOT NULL,
  config_sha       TEXT REFERENCES configs (config_sha),
  completions      INTEGER, parallelism INTEGER,
  node_pool        TEXT,
  cpu_request      TEXT, cpu_limit TEXT,
  mem_request_gib  REAL, mem_limit_gib REAL,
  localtmp_gib     REAL, pod_deadline_s INTEGER,
  state            TEXT NOT NULL DEFAULT 'pending',   -- pending | running | succeeded | failed | deleted (sticky once terminal)
  created_ms       INTEGER, finished_ms INTEGER,
  n_succeeded      INTEGER, n_failed INTEGER, failed_indexes TEXT,
  observed_ms      INTEGER NOT NULL
) STRICT;
CREATE INDEX jobs_run ON jobs (run_uid, created_ms);

CREATE TABLE job_tiles (                              -- which index ran which tile
  job_id  TEXT NOT NULL,
  idx     INTEGER NOT NULL,
  tile_id TEXT NOT NULL,
  PRIMARY KEY (job_id, idx)
) STRICT, WITHOUT ROWID;
CREATE INDEX job_tiles_tile ON job_tiles (tile_id);

CREATE TABLE pods (                                   -- one row per attempt
  pod_id            TEXT PRIMARY KEY,                 -- '<job_id>/<pod_name>'
  job_id            TEXT NOT NULL REFERENCES jobs (job_id),
  pod_name          TEXT NOT NULL,
  idx               INTEGER,
  tile_id           TEXT,
  node_name         TEXT, node_pool TEXT, machine_type TEXT,
  phase             TEXT NOT NULL DEFAULT 'Pending',  -- Pending | Running | Succeeded | Failed (sticky once terminal)
  reason            TEXT,                             -- Completed | OOMKilled | Error | Evicted ...
  exit_code         INTEGER,
  created_ms        INTEGER, started_ms INTEGER, finished_ms INTEGER,
  pending_s         REAL, duration_s REAL,
  -- summaries, filled when the pod ends; they survive any later pruning of samples
  peak_mem_gib      REAL, peak_mem_pct REAL,           -- memory.current: includes page cache
  peak_anon_gib     REAL, peak_anon_pct REAL,          -- the heap: what an OOM kill is about
  peak_io_psi       REAL, peak_mem_psi REAL, peak_cpu_psi REAL,
  peak_localtmp_pct REAL, total_write_gib REAL, cpu_throttled_s REAL,
  oom_kill_count    INTEGER,
  n_samples         INTEGER,
  cost_usd          REAL,
  verdicts          TEXT,                             -- codes, comma separated
  observed_ms       INTEGER NOT NULL,
  UNIQUE (job_id, pod_name)
) STRICT;
CREATE INDEX pods_job ON pods (job_id, idx);
CREATE INDEX pods_tile ON pods (tile_id, created_ms DESC);
CREATE INDEX pods_near_limit ON pods (peak_anon_pct DESC) WHERE peak_anon_pct >= 80;

CREATE TABLE samples (                                -- the pod's own resource_sample lines, ~4 per minute
  pod_id            TEXT NOT NULL,
  ts_ms             INTEGER NOT NULL,
  mem_current_gib   REAL, mem_pct REAL,
  mem_anon_gib      REAL, mem_anon_pct REAL,
  io_psi            REAL, mem_psi REAL, cpu_psi REAL,
  cpu_usage_s       REAL, cpu_throttled_s REAL,
  write_bytes       INTEGER,
  localtmp_used_pct REAL,
  PRIMARY KEY (pod_id, ts_ms)
) STRICT, WITHOUT ROWID;

CREATE TABLE events (                                 -- Kubernetes events (the cluster keeps them about an hour)
  event_id TEXT PRIMARY KEY,                          -- sha1 of time, pod, reason and message
  run_uid  TEXT NOT NULL,
  pod_name TEXT,
  type     TEXT NOT NULL,                             -- Normal | Warning
  reason   TEXT NOT NULL,
  message  TEXT,
  first_ms INTEGER NOT NULL, last_ms INTEGER NOT NULL, count INTEGER NOT NULL DEFAULT 1
) STRICT;
CREATE INDEX events_run ON events (run_uid, first_ms);

CREATE TABLE log_objects (                            -- pointers to archived pod logs
  uri     TEXT PRIMARY KEY,
  pod_id  TEXT NOT NULL,
  kind    TEXT NOT NULL DEFAULT 'ndjson',
  lines   INTEGER
) STRICT;

CREATE TABLE ingested_objects (                       -- makes `sync` idempotent and resumable
  uri         TEXT PRIMARY KEY,
  ingested_ms INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
