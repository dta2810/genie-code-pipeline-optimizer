---
name: sandbox-setup
description: Build an isolated sandbox to run an optimization candidate safely. Shallow-clones the notebook's target tables into a dedicated sandbox schema (WITH current data for MERGE/UPDATE), pins source tables via Delta time-travel, and remaps all writes to the sandbox. Guarantees production is never touched. Use before running a v2 notebook end-to-end.
---

# sandbox-setup

Import `scripts/setup_sandbox.py` — do NOT reimplement inline.

Inputs: notebook entry from `opt_config` (`source_tables`, `target_tables`, `operation`, `sandbox_schema`).

Steps:
1. **Pin inputs**: resolve each `source_table` to a fixed Delta version (`DESCRIBE HISTORY` →
   latest committed version) so baseline (v1) and candidate (v2) read identical data.
2. **Clone targets** (`clone_targets`): always `CREATE OR REPLACE` — **never `DROP`** (workspace
   policies may block DROP). Two modes:
   - **`validation_tier: sampled` (DEFAULT for large targets)** → pass `sample_percent` so the clone
     is a deterministic `TABLESAMPLE (n PERCENT) REPEATABLE(seed)` snapshot. A full-rewrite v1 on a
     180M-row target times out / cold-starts on serverless — a sampled slice validates equivalence
     fast. Same seed ⇒ _v1/_v2 snapshots identical. **Sample the big TARGET only; pin sources full.**
   - **`validation_tier: full`** → `SHALLOW CLONE` (zero-copy, whole table). Use on dedicated compute.
   In both, the clone stays in the target's OWN catalog + sandbox schema, `<origschema>__<table>`
   name, WITH data (empty target ⇒ wrong merge result).
3. **Remap writes**: rewrite the v2 notebook's write targets to their sandbox clones. Verify no
   write points at a production table.
4. **Persist the manifest** (pinned versions + clone names) to `opt_config`/a run row — do NOT keep
   sandbox handles only in notebook cell variables. Serverless compute can restart mid-run and wipe
   in-memory state; a persisted manifest lets the flow resume without re-cloning.
5. `audit_log(step="sandbox_setup", insight=...)` with the pinned versions + clone names + tier.

Output: a sandbox manifest (pinned input versions + clone table names) consumed by
equivalence-check and perf-benchmark.
