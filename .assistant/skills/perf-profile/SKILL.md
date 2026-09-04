---
name: perf-profile
description: Diagnose the performance bottleneck of a job's notebooks. Reads the job JSON (Jobs API), query history / query profile, and Spark metrics from system tables; ranks notebooks/steps by runtime x frequency x cost and identifies the cause (skew, spill, exploding join, full scan, small files, UDF, missing pruning). Use FIRST, before proposing any optimization.
---

# perf-profile

Import `scripts/profile_perf.py` — do NOT reimplement inline.

Inputs: `job_name` (→ Jobs API JSON), `opt_config`.

Steps:
1. Pull the job JSON; list tasks/notebooks in DAG order.
2. For each notebook, gather runtime + cost from `system.query.history` / `system.billing` and
   Spark SQL metrics (shuffle read/write, spill, bytes/files scanned, task-time skew).
3. Rank by `runtime x frequency x cost`; pick the hotspot step.
4. Attribute the cause via the query profile / `EXPLAIN` — but report **measured runtime**, not
   the plan (EXPLAIN != runtime).
5. Write the bottleneck profile to the factory schema + `audit_log(step="perf_profile", insight=...)`.

Output: a structured bottleneck profile + a short NL insight (where and why it is slow).
