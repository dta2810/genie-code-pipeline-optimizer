-- Governance views over optimization_audit. {{catalog}}/{{schema}} substituted by deploy.

-- Full run trail (with the NL insight).
CREATE OR REPLACE VIEW {{catalog}}.{{schema}}.v_run_activity
COMMENT 'Every optimization step, newest first, with its natural-language insight.'
AS SELECT event_ts, user, job, notebook, step, status, change_type, equivalence, perf, insight
   FROM {{catalog}}.{{schema}}.optimization_audit
   ORDER BY event_ts DESC;

-- Guards that fired: failed equivalence / perf steps = evidence a bad candidate was blocked.
CREATE OR REPLACE VIEW {{catalog}}.{{schema}}.v_guard_events
COMMENT 'Failed steps — equivalence/perf gates that blocked a candidate.'
AS SELECT event_ts, job, notebook, step, equivalence, perf, insight
   FROM {{catalog}}.{{schema}}.optimization_audit
   WHERE status = 'failed'
   ORDER BY event_ts DESC;

-- One row per (job, notebook): latest status + latest equivalence + latest perf.
CREATE OR REPLACE VIEW {{catalog}}.{{schema}}.v_optimization_scorecard
COMMENT 'Latest verdict per job/notebook: equivalence result, perf gain, status.'
AS WITH a AS (
     SELECT *, ROW_NUMBER() OVER (PARTITION BY job, notebook, step ORDER BY event_ts DESC) rn
     FROM {{catalog}}.{{schema}}.optimization_audit
   )
   SELECT
     c.job_name AS job, c.notebook_path AS notebook, c.status AS config_status,
     eq.equivalence['result'] AS equivalence_result,
     pf.perf['gain'] AS perf_gain, pf.perf['passed'] AS perf_passed,
     GREATEST(COALESCE(eq.event_ts, TIMESTAMP('1970-01-01')),
              COALESCE(pf.event_ts, TIMESTAMP('1970-01-01'))) AS last_event_ts
   FROM {{catalog}}.{{schema}}.opt_config c
   LEFT JOIN (SELECT * FROM a WHERE step = 'equivalence' AND rn = 1) eq
     ON c.job_name = eq.job AND c.notebook_path = eq.notebook
   LEFT JOIN (SELECT * FROM a WHERE step = 'perf_benchmark' AND rn = 1) pf
     ON c.job_name = pf.job AND c.notebook_path = pf.notebook;
