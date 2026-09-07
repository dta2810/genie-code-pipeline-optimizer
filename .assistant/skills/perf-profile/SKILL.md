---
name: perf-profile
description: Deep root-cause diagnosis of ONE selected notebook's bottleneck. Reads the job JSON (Jobs API), query history / query profile, and Spark metrics from system tables; identifies the cause (skew, spill, exploding join, full scan, small files, UDF, missing pruning) and maps it to an optimization-catalog technique. Runs AFTER the user selects the notebook (the light cross-notebook hotspot ranking is lib.perf.rank_notebooks).
---

# perf-profile

Import `scripts/profile_perf.py` — do NOT reimplement inline. This is the **DEEP** root-cause dive
on the ONE notebook the user selected — not the cross-notebook hotspot ranking (that is the light
`lib.perf.rank_notebooks` pass the orchestrator runs before selection).

Inputs: the selected `notebook_path`, `job_name` (→ Jobs API JSON), `opt_config`.

Steps:
1. For the selected notebook, gather runtime + cost from `system.query.history` / `system.billing`
   and Spark SQL metrics (shuffle read/write, spill, bytes/files scanned, task-time skew).
2. Within the notebook, find the slowest operation/step (the query profile pinpoints it).
3. Attribute the cause via the query profile / `EXPLAIN` — but report **measured runtime**, not
   the plan (EXPLAIN != runtime). Map each cause to a technique using the `optimization-catalog`
   symptom→technique table — AND, when they fit better, techniques from your other Databricks skills
   (`writing-sql`, `table-optimization`, `data-modification`, `performance-tuning`). The catalog is a
   starting set, not a limit. Name the technique(s) and their source in the suggestion so
   optimize-notebook can apply them. (The forbidden protocol rule + gates apply to every source.)
4. Write the bottleneck profile to the factory schema + `audit_log(step="perf_profile", insight=...)`.

Output: a structured bottleneck profile + a short NL insight (where and why it is slow).
