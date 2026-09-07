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
2. **Clone targets**: `CREATE TABLE <catalog>.<sandbox_schema>.<origschema>__<t> SHALLOW CLONE <prod>`
   for each `target_table` — the clone stays in the target's OWN catalog, in the dedicated sandbox
   schema, with the original schema folded into the table name to avoid collisions. Shallow clone
   is zero-copy and carries current data — required so a MERGE behaves exactly like prod.
   (Empty target => wrong merge result.)
3. **Remap writes**: rewrite the v2 notebook's write targets to their sandbox clones (same
   catalog, sandbox schema, `<origschema>__<table>` name). Verify no write points at a production table.
4. `audit_log(step="sandbox_setup", insight=...)` with the pinned versions + clone names.

Output: a sandbox manifest (pinned input versions + clone table names) consumed by
equivalence-check and perf-benchmark.
