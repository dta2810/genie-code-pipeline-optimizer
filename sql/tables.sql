-- Optimizer schema DDL. {{catalog}}/{{schema}} substituted by deploy from settings env vars.

CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{schema}};

-- opt_config: one row per (job, notebook), auto-seeded from the job JSON, human-reviewed.
CREATE TABLE IF NOT EXISTS {{catalog}}.{{schema}}.opt_config (
  job_name           STRING,
  job_id             STRING,
  notebook_path      STRING,
  task_key           STRING,          -- job task key (attributes runtime for hotspot ranking)
  dag_order          INT,
  operation          STRING,          -- insert | merge | update | ctas | ...
  source_tables      ARRAY<STRING>,   -- pinned via time-travel
  target_tables      ARRAY<STRING>,   -- shallow-cloned (with data) to the sandbox schema
  equivalence_keys   ARRAY<STRING>,
  sandbox_schema     STRING,          -- dedicated schema (inside each target's own catalog)
  optimized_folder   STRING,
  epsilon            DOUBLE,
  min_gain           DOUBLE,
  benchmark_runs     INT,
  compute_cluster_id STRING,          -- dedicated cluster for heavy sandbox runs (governed; NULL = serverless session)
  nondeterministic   BOOLEAN,
  status             STRING           -- pending|skipped|proposed|approved|validated|promoted|blocked
) USING DELTA;

-- optimization_audit: append-only trail, started -> terminal per step, with an NL insight.
CREATE TABLE IF NOT EXISTS {{catalog}}.{{schema}}.optimization_audit (
  audit_id        STRING,
  event_ts        TIMESTAMP,
  user            STRING,
  job             STRING,
  notebook        STRING,
  step            STRING,          -- perf_profile|sandbox_setup|generate_v2|equivalence|perf_benchmark|promote
  status          STRING,          -- started|succeeded|failed
  change_type     ARRAY<STRING>,
  equivalence     MAP<STRING,STRING>,  -- method, rows_compared, result
  perf            MAP<STRING,STRING>,  -- runtime_before/after, dbu, shuffle, spill, gate
  notebook_path   STRING,
  insight         STRING           -- short NL: the WHY
) USING DELTA;
