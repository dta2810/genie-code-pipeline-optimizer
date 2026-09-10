---
name: optimize-notebook
description: Generate an optimized v2 of a notebook after the human approves the suggestions. Writes a NEW notebook named <ntb>_genie_opt_<timestamp> in the dedicated optimized_folder, applying the approved techniques from optimization-catalog (broadcast/skew/de-UDF/clustering/small-files/incremental-MV). Never edits the original. Writes are already remapped to the sandbox by sandbox-setup.
---

# optimize-notebook

Import `scripts/generate_v2.py` — do NOT reimplement inline. Requires human approval of the
suggestions (GATE 1) BEFORE running.

Inputs: original notebook, approved suggestions (each names a technique), `optimized_folder`.

Steps:
1. Copy the original into `<optimized_folder>/<ntb>_genie_opt_<timestamp>` — never edit the source.
2. For each approved technique, apply its recipe: from `optimization-catalog/resources/<technique>.md`
   if it's a catalog technique, or from the relevant Databricks skill (`writing-sql`,
   `table-optimization`, `data-modification`, `performance-tuning`) if it came from there. The
   catalog is a starting set, not a limit — use the best valid technique for the notebook, whatever
   its source. Don't reimplement a documented recipe inline; follow the skill's guidance.
3. Apply one transformation per logical step, keeping intermediate outputs materializable for
   step-by-step equivalence. Honor the technique's equivalence-risk tier (semantics-preserving
   rewrites like de-UDF/salting demand a full, unsampled gate downstream).
   **NEVER apply a protocol/table-feature bump** (liquid `CLUSTER BY`, deletion vectors, row
   tracking, generated columns, v2 checkpoint) — forbidden by the catalog rule and blocked by
   `assert_no_protocol_change`. For `CREATE OR REPLACE TABLE`, carry over the source table's
   TBLPROPERTIES/protocol so modern engine defaults don't silently bump it.
   **⚠️ Partition reload — use `REPLACE WHERE`, NOT a bare `INSERT OVERWRITE`.** A bare
   `INSERT OVERWRITE TABLE t SELECT * FROM src` is a **STATIC, full-table** overwrite by default — it
   replaces the WHOLE table with the SELECT, silently deleting every partition not in `src`. (Verified
   the hard way: it shrank a 40M-row / 60-day table to the 700K / 1-day reload = 39.3M rows lost, and
   the job still ran green.) **It is NOT partition-scoped by default** — do not assume it is.
   `spark.sql.sources.partitionOverwriteMode=dynamic` would fix it, but `spark.conf.set(...)` raises
   `CONFIG_NOT_AVAILABLE` on serverless / Spark Connect. The correct, Spark-Connect-safe equivalent of a
   `DELETE partitions-in-src + INSERT src` is an atomic replace scoped to the reloaded partitions —
   **but `REPLACE WHERE` does NOT accept a subquery** (`UNSUPPORTED_FEATURE.OVERWRITE_BY_SUBQUERY`), so
   read the partition keys FIRST and embed them as LITERALS:
   ```python
   days = [r[0] for r in spark.sql(f"SELECT DISTINCT FECHA_ID FROM {src}").collect()]
   if days:
       in_list = ",".join(str(int(d)) for d in days)   # quote if the key is a string
       spark.sql(f"INSERT INTO {t} REPLACE WHERE FECHA_ID IN ({in_list}) SELECT * FROM {src}")
   ```
   Empirically verified equivalent to `DELETE+INSERT` on 40M rows / 60 partitions (0/0), no session conf.
   The equivalence gate must compare against the FULL original table (all partitions), not just the
   reloaded one, or it will miss this exact data-loss bug.
4. Keep the write targets as configured so `sandbox-setup` remaps them to the sandbox.
5. `audit_log(step="generate_v2", change_type=<list of techniques>, insight=<what changed + why faster>)`.

Output: the v2 notebook path, ready for sandbox-setup → run → equivalence-check → perf-benchmark.
