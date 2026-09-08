---
name: perf-profile
description: Deep root-cause diagnosis of ONE selected notebook's bottleneck. Reads the job JSON (Jobs API), query history / query profile, and Spark metrics from system tables; identifies the cause (skew, spill, exploding join, full scan, small files, UDF, missing pruning) and maps it to an optimization-catalog technique. Runs AFTER the user selects the notebook (the light cross-notebook hotspot ranking is lib.perf.rank_notebooks).
---

# perf-profile

Import `scripts/profile_perf.py` — do NOT reimplement inline. This is the **DEEP** root-cause dive
on the ONE notebook the user selected — not the cross-notebook hotspot ranking (that is the light
`lib.perf.rank_notebooks` pass the orchestrator runs before selection).

Inputs: the selected `notebook_path`, its `target_tables` (from detect-tables), `opt_config`.

**Get the signals from data, portably (no logfood).** Call `profile(spark, job=, notebook=,
target_tables=)` — it reads two sources available in any workspace and audits the result:
- **`system.query.history`** — `spilled_local_bytes` (SPILL), `read_bytes`/`read_rows`/`produced_rows`
  (bytes scanned + row amplification = exploding-join signal), and the time breakdown
  (compilation vs execution). Matched to the notebook by its target-table names.
- **Delta `DESCRIBE HISTORY` operationMetrics** — files/rows added vs removed + scan/rewrite time =
  the **full-rewrite-vs-delta-touch** pattern (the INSERT OVERWRITE / DELETE+INSERT waste).

**Honest caveat on SHUFFLE:** per-stage shuffle bytes are **not** columns in `system.query.history`;
they live in the query profile / Spark UI. Report spill + the indirect signals, and flag
shuffle/skew as **"suspected — confirm in the query profile"** rather than quoting a number you don't
have. For the real shuffle/operator detail, use the query profile (or, for internal FE analysis only,
`vadim-lite` / the query-profile toolkit) — do NOT block on it.

Steps:
1. `profile(...)` → the portable signals + a '4 S's' diagnosis (spill, row amplification, full-rewrite,
   suspected shuffle/skew). Report **measured** numbers, not the plan (EXPLAIN != runtime).
2. Map each flagged cause to a technique via the `optimization-catalog` symptom→technique table — AND,
   when they fit better, techniques from your other Databricks skills (`writing-sql`,
   `table-optimization`, `data-modification`, `performance-tuning`). The catalog is a starting set, not
   a limit. Name the technique(s) and their source so optimize-notebook can apply them. (The forbidden
   protocol rule + gates apply to every source.)
3. `profile(...)` already audits `step="perf_profile"` with the `perf` map (spill/bytes/amplification/
   flags) + a short NL insight. Present the flags to the human at GATE 1.

Output: a structured bottleneck profile + a short NL insight (where and why it is slow).

**Propose the FULL applicable stack up front — proactively, not on request.** For the hotspot, list
every technique that applies (catalog + other-skill), each tagged with its risk tier — e.g. a
full-rewrite upsert on a big table warrants MERGE (the core fix) **plus** Z-ORDER on the merge key,
`optimizeWrite`/`autoCompact`, and a BROADCAST hint. Don't surface just one and wait for the user to
ask "what else?" — present the stack, let the human pick at GATE 1. All still pass the same gates.
