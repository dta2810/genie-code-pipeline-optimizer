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
   policies may block DROP). **Always call `clone_targets` — NEVER hand-roll the clone.** A hand-written
   `CREATE TABLE ... PARTITIONED BY (col) AS SELECT * ... TABLESAMPLE` is broken two ways: the
   `PARTITIONED BY` reprojects the partition column (it can land NULL/misaligned), and a plain
   `TABLESAMPLE` is **non-deterministic between statements**, so the _v1 and _v2 clones get different
   rows and equivalence can never match. `clone_targets` avoids both (SHALLOW CLONE, or a `REPEATABLE(seed)`
   sample). Pick the mode by the **operation**, not just table size:

   | v2 operation | tier | why |
   |---|---|---|
   | **Partition reload / partition-scoped** — DELETE-partition+INSERT, dynamic-partition INSERT OVERWRITE, `REPLACE WHERE` | **`full` (SHALLOW CLONE) — REQUIRED** | the op is already partition-bounded and cheap (it touches one partition, not the whole table), and it DEPENDS on the clone's partitioning. Sampling drops the `PARTITIONED BY` and can NULL/misalign the partition column → equivalence diverges even though the v2 logic is correct. |
   | **Full-table rewrite** — INSERT OVERWRITE whole table, self-join upsert that rewrites everything | **`sampled`** | the baseline materializes the ENTIRE table, which times out on serverless; a `TABLESAMPLE (n PERCENT) REPEATABLE(seed)` slice validates the logic fast. Same seed ⇒ _v1/_v2 identical. Sample the big TARGET only; pin sources full. |
   | small target (either op) | **`full` (SHALLOW CLONE)** | cheap; exact is best. |

   `full` → `SHALLOW CLONE` (zero-copy metadata clone; **preserves partitioning and exact data**).
   `sampled` → `clone_targets(..., sample_percent=n)`. The clone stays in the target's OWN catalog +
   sandbox schema, `<origschema>__<table>` name, WITH data (empty target ⇒ wrong merge result).

   **Sample size for pathological baselines:** equivalence proves LOGICAL equality, not scale — so the
   sample only needs enough rows to exercise every code path. If the v1 baseline is algorithmically
   expensive (e.g. `NOT IN` on a composite-key tuple → `BroadcastNestedLoopJoin` LeftAnti, which is
   O(n×m)), even a 1% slice can run for **over an hour** — that cost is the quadratic algorithm, NOT
   the data volume and NOT the sample being "too high". Drop the sample much lower (≤0.1% or a fixed
   small cap) so the slow baseline finishes; the candidate (MERGE) is fast at any size, and the proof
   still holds. That catastrophic v1 time is exactly the waste the optimization removes — report it as
   the finding, don't fight it at scale.
3. **Remap writes**: rewrite the v2 notebook's write targets to their sandbox clones. Verify no
   write points at a production table.
4. **Persist the manifest** (pinned versions + clone names) to `opt_config`/a run row — do NOT keep
   sandbox handles only in notebook cell variables. Serverless compute can restart mid-run and wipe
   in-memory state; a persisted manifest lets the flow resume without re-cloning.
5. `audit_log(step="sandbox_setup", insight=...)` with the pinned versions + clone names + tier.

Output: a sandbox manifest (pinned input versions + clone table names) consumed by
equivalence-check and perf-benchmark.
