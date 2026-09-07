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

**0. Bootstrap config.** From the user's job name:
```python
from lib.config import bootstrap_from_job, sync_config
cfg = bootstrap_from_job("<job_name>")   # Jobs API -> notebooks in DAG order
sync_config(spark, cfg)                  # persist to opt_config
```
Show the drafted config (notebooks + compute + sandbox_schema) and confirm the job with the user.

**Per notebook, in DAG order:**

1. **`@detect-tables`** — read the notebook, reason out `source_tables` (to pin) + `target_tables`
   (to clone) + `operation`, resolve dynamic names, flag non-determinism. Write them into the
   notebook's `opt_config` row. If non-deterministic or a name is unresolved → STOP, report.
2. **`@perf-profile`** — diagnose the bottleneck (job JSON + system tables + query profile).
   Report *where* and *why* it is slow; report measured runtime, not just the plan.
3. **Propose** concrete optimizations for this notebook. → **GATE 1: wait for human approval.**
4. **`@optimize-notebook`** — write v2 to `<optimized_folder>/<ntb>_genie_opt_<timestamp>`; never
   edit the original.
5. **`@sandbox-setup`** — shallow-clone targets into a dedicated `sandbox_schema` in each target's
   own catalog (WITH data for MERGE), pin inputs via time-travel, remap all writes to the sandbox.
   Verify no write hits production.
6. **Run** the baseline (v1) and the candidate (v2) end-to-end against the sandbox on the same compute.
7. **`@equivalence-check`** — counts → column fingerprint → `EXCEPT ALL` both ways, step-by-step,
   with `epsilon`. HARD gate: any divergence → `validate=failed`, write insight, STOP (no promotion).
8. **`@perf-benchmark`** — median of N runs; promote only if equivalent AND faster by `min_gain`.
9. **`@security-review`** — LATE security gate on the v2 (secrets, injection, access/PII broadening,
   writes outside the sandbox, unsafe UDF/external calls, cost blowups). Any finding → STOP, no promotion.
10. **GATE 2: wait for human approval** before promoting (PR/DAB back into the job).

## Audit contract (every step)
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
