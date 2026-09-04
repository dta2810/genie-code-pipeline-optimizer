-- get_opt_config: read a job's opt_config rows (SQL/agent access). {{catalog}}/{{schema}} substituted by deploy.

CREATE OR REPLACE FUNCTION {{catalog}}.{{schema}}.get_opt_config(p_job STRING)
RETURNS TABLE (
  job_name STRING, notebook_path STRING, dag_order INT, operation STRING,
  source_tables ARRAY<STRING>, target_tables ARRAY<STRING>, equivalence_keys ARRAY<STRING>,
  sandbox_catalog STRING, optimized_folder STRING, epsilon DOUBLE, min_gain DOUBLE,
  benchmark_runs INT, nondeterministic BOOLEAN, status STRING
)
COMMENT 'Return the optimization config rows for a job, in DAG order.'
RETURN
  SELECT job_name, notebook_path, dag_order, operation, source_tables, target_tables,
         equivalence_keys, sandbox_catalog, optimized_folder, epsilon, min_gain,
         benchmark_runs, nondeterministic, status
  FROM {{catalog}}.{{schema}}.opt_config
  WHERE job_name = p_job
  ORDER BY dag_order;
