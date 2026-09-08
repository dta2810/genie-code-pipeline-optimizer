---
name: optimize-pipeline
description: Entry point for the Pipeline Optimization Factory. Invoke with @optimize-pipeline to optimize a Databricks job's notebooks safely and provably — the same result as the baseline (before == after) and faster/cheaper, every step audited. Give it a job name. Self-contained: it orchestrates detect-tables, perf-profile, sandbox-setup, optimize-notebook, equivalence-check and perf-benchmark, with two human approval gates. Use this instead of relying on project instructions.
---

# optimize-pipeline (orchestrator)

Invoke: `@optimize-pipeline optimicemos el pipeline <job_name>`.

This skill is **self-contained** — it carries the full flow, the human gates, the audit
contract and the guardrails, so you do NOT depend on any workspace `.assistant_instructions.md`.
Work **one job at a time**, **one notebook at a time**. Import the harness from `lib/` and the
sub-skills' `scripts/` — do NOT reimplement their logic inline.

## Flow

**0. Bootstrap config.** First point the harness at THIS deployment's factory (env vars set on a
laptop do not reach the Databricks runtime), then draft the config from the job name:
```python
from lib import settings
settings.configure(spark=spark)          # explicit > config file > AUTO-DISCOVERS an existing factory > defaults
print(settings.resolved())               # confirm it points at the DEPLOYED factory, not defaults
# (or pass them explicitly: settings.configure(factory_catalog=..., factory_schema=...))

from lib.config import bootstrap_from_job, select_notebooks, sync_config, pending_notebooks
from lib.perf import rank_notebooks
from lib.compute import resolve_compute
cfg = bootstrap_from_job("<job_name>")   # Jobs API -> notebooks in DAG order
print(resolve_compute(cfg))              # which cluster runs the HEAVY sandbox work
```
Do NOT provision the factory if `resolved()` already points at a real deployed schema — provisioning
is a one-time deploy step (`deploy/00_deploy`), and it now refuses to create a duplicate anyway.
Only provision on a genuinely fresh workspace.
Show the drafted config (notebooks + compute + sandbox_schema) and confirm the **job** with the user.

**Compute routing.** Control work (bootstrap, detect-tables, audit/config writes, equivalence on the
*sampled* sandbox) runs on THIS serverless session — cheap and bounded. The HEAVY work — running the
v1/v2 notebooks end-to-end and the wall-clock benchmark — is submitted to a **dedicated cluster** via
the Jobs API, because serverless cold-starts and loses cell state on autoscale (a large rewrite times
out) and its wall-clock is contaminated (setup, not I/O). `resolve_compute(cfg)` picks it: explicit
override > `opt_config.compute_cluster_id` (governed) > the job's own compute > the settings default >
serverless session. If it resolves to `serverless_session`, WARN the user that large runs may time out
and wall-clock is indicative-only.

**0a. Rank the hotspots (light, read-only).** Before asking the user to choose, cheaply rank the
notebooks by cost so the recommendation is data-driven — not a blind pick.
```python
rank = rank_notebooks(spark, cfg["job_id"], cfg["notebooks"])   # runtime x freq from system tables
```
This only READS `system.lakeflow.job_task_run_timeline` — no writes, negligible cost. It is the
LIGHT pass; `perf-profile` (step 2) does the DEEP root-cause dive on the chosen notebook only.

**0b. SELECT which notebooks to optimize this run (hybrid: recommend + override).** Present the
notebooks ranked by `cost_share`, and **recommend the top hotspot**:
> "`5_gold_ingestion` is 58% of the job's runtime over the last 30 days — recommend starting there.
> Optimize that one, or pick another?"
The user accepts the recommendation or names another (gradual — steer toward one, or a small set, to
start). If `rank["ranked"]` is False (no telemetry), just present the DAG-ordered list and let the
user pick.
```python
pick = rank["recommended"]["dag_order"] if rank.get("recommended") else <user's choice>
select_notebooks(cfg, picks=pick)         # a dag_order, a name substring, or a list; None = all
sync_config(spark, cfg)                    # persist: selected -> pending, the rest -> skipped
```
Selected notebooks are `pending`; the rest are `skipped` (persisted, so a later run can pick them
up — `promoted`/`blocked` history is never overwritten).

**For each SELECTED notebook (`pending_notebooks(cfg)`), in DAG order:**

1. **`@detect-tables`** — read the notebook, reason out `source_tables` (to pin) + `target_tables`
   (to clone) + `operation`, resolve dynamic names, flag non-determinism. Write them into the
   notebook's `opt_config` row. If non-deterministic or a name is unresolved → STOP, report.
2. **`@perf-profile`** — diagnose the bottleneck (job JSON + system tables + query profile).
   Report *where* and *why* it is slow; report measured runtime, not just the plan.
3. **Propose** concrete optimizations for this notebook, and state which compute will run the
   sandbox (`resolve_compute(cfg)`). The user may override the cluster for this run (pass `override=`)
   or accept the governed default. → **GATE 1: wait for human approval.**
4. **`@optimize-notebook`** — write v2 to `<optimized_folder>/<ntb>_genie_opt_<timestamp>`; never
   edit the original.
5. **`@sandbox-setup`** — shallow-clone targets into a dedicated `sandbox_schema` in each target's
   own catalog (WITH data for MERGE), pin inputs via time-travel, remap all writes to the sandbox.
   Verify no write hits production.
6. **Run** the baseline (v1) and the candidate (v2) end-to-end against the sandbox on the **same
   compute** — the dedicated cluster from `resolve_compute(cfg)`, not this serverless session.
   Materialize the two sandbox-remapped notebooks to the workspace and submit each on the cluster:
   ```python
   from lib.compute import resolve_compute, run_notebook
   cid = resolve_compute(cfg)["cluster_id"]
   if cid:
       run_notebook(v1_sandbox_path, cid)   # jobs runs submit; clean execution_duration
       run_notebook(v2_sandbox_path, cid)
   # else: run inline on the session (sampled sandbox only) and label wall-clock indicative-only
   ```
7. **PROTOCOL gate + `@equivalence-check`.** First prove the candidate did not change the table
   contract, then prove the content matches:
   ```python
   from lib.equivalence import assert_no_protocol_change
   assert_no_protocol_change(spark, v1_clone, v2_clone)   # RAISES on any reader/writer/feature bump
   ```
   Then counts → column fingerprint → `EXCEPT ALL` both ways, step-by-step, with `epsilon`. Both are
   HARD gates: any protocol bump or divergence → write insight, STOP (no promotion).
8. **`@perf-benchmark`** — median of N runs **on the dedicated cluster** (`benchmark_on_cluster`, warm,
   `execution_duration` only); report the gain vs `min_gain`. A gain below `min_gain` is a **signal to
   the human, not an automatic block** — surface it clearly. Equivalence is the hard gate; perf is
   advisory. If it resolves to serverless, do NOT quote wall-clock — report the structural I/O signal
   (rows/files rewritten) instead. If the human promotes a below-threshold candidate for correctness or
   maintainability (not raw speed), say so explicitly and record that rationale in the audit insight.
9. **`@security-review`** — LATE security gate on the v2 (secrets, injection, access/PII broadening,
   writes outside the sandbox, unsafe UDF/external calls, cost blowups). Any finding → STOP, no promotion.
10. **NO-AUDIT-NO-PROMOTE gate.** Before offering promotion, prove the run was actually recorded:
    ```python
    from lib.audit import assert_audited, audit_trail
    assert_audited(spark, job=job_name, notebook=notebook_path)   # RAISES if the trail is missing
    display(spark.createDataFrame(audit_trail(spark, job=job_name, notebook=notebook_path)))
    ```
    If this raises, the harness was bypassed (steps run inline) — re-run through the wrappers. Show
    the trail to the human as evidence.
11. **GATE 2: wait for human approval**, then promote via the Jobs JSON (this iteration: NO PR/DAB):
    ```python
    from lib.promote import promote_notebook
    promote_notebook(job_id, task_key, v2_path, new_job=True)   # new job; original untouched
    ```
    `new_job=True` clones the job with the task repointed to the v2 (rollback = delete the new job);
    `new_job=False` updates the job in place. Record the outcome with `audit_log(step="promote", ...)`.

## Audit contract (every step)
**Use the harness wrappers — do NOT reimplement their logic inline.** The wrappers (`lib/` +
`scripts/`) are what write `opt_config` (`sync_config` after selection) and append the audit trail
(`audit_step`/`audit_log`). If you run the steps with your own inline code, `opt_config` and
`optimization_audit` stay EMPTY and the NO-AUDIT-NO-PROMOTE gate (step 10) will refuse to promote.
`audit_log()` with started → terminal (succeeded, or failed + re-raise). Steps: detect_tables,
perf_profile, generate_v2, sandbox_setup, equivalence, perf_benchmark, security_review, promote.
Record job, notebook, step, status, change_type, equivalence (method + rows compared + result),
perf (runtime/DBU/shuffle/spill/gate), and a short NL **insight** (the *why*). Insight on every
equivalence result, every security finding, and every failure.

## Guardrails
- The baseline is the reference truth; prove the candidate equivalent **to it**.
- Never soften `epsilon` or `min_gain` to force a pass — if a threshold is wrong, change it in
  `opt_config` with justification, not the check.
- Never write to a production table. MERGE/UPDATE targets are cloned WITH data, never empty.
- Neutral wording in insights ("pipeline owner", "data engineer").
