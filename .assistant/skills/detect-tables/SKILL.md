---
name: detect-tables
description: Identify every table a notebook READS (source_tables, to pin) and every table it WRITES (target_tables, to clone), by reading and reasoning over the notebook code — no static parser. Resolves dynamic/parameterized names, excludes temp views/CTEs, classifies the write operation, and cross-checks against Unity Catalog lineage. Run before sandbox-setup; a human confirms the lists before anything is cloned or pinned.
---

# detect-tables

**You (Genie Code) do this by reading the notebook — do NOT write a parser.** Reason over the
code the way an engineer would, then verify. Output goes into `opt_config` for that notebook.

## What to produce
For the notebook:
- `source_tables[]` — every Unity Catalog table the notebook **reads** (will be pinned via time-travel).
- `target_tables[]` — every UC table the notebook **writes**, each with its `operation`
  (`insert` | `merge` | `update` | `ctas` | `overwrite`).
- `nondeterministic` — true if any output depends on time/random/unordered logic (see §Flags).
- `unresolved[]` — any table reference you could not resolve to a concrete name.

## How to read the code
1. Read the WHOLE notebook top to bottom, including imported helpers and any `%run` includes.
2. **Reads** = `FROM` / `JOIN` in SQL, `spark.read.table` / `spark.table` / `read_files` / DLT
   `read` / `readStream`. **Writes** = `INSERT INTO`, `MERGE INTO`, `UPDATE`, `CREATE [OR REPLACE]
   TABLE ... AS`, `.write.saveAsTable` / `.saveAsTable` / `insertInto` / `writeStream ... .table`.
3. **Resolve dynamic names**: trace widgets (`dbutils.widgets.get`), variables, f-strings,
   `${param}`, and config lookups back to their concrete value. This is exactly what a static
   parser can't do — use your understanding of the code flow. If a value depends on a runtime
   input you can't determine, put it in `unresolved[]` and STOP (ask the human).
4. **Exclude non-tables**: temp views, global temp views, CTEs, and DataFrames are NOT tables —
   never pin or clone them. Only real UC tables (`catalog.schema.table`) go in the lists.
5. **Self-referencing**: a table both read and written (e.g. a self-`MERGE`) belongs in BOTH lists.

## Verify (safety net, not a parser)
6. Cross-check your lists against UC lineage:
   `system.access.table_lineage` filtered by the notebook/entity → confirm the writes and catch
   any read you missed. If lineage shows a table you didn't list, investigate and reconcile.
7. Confirm each `target_table` is a **managed Delta** table (shallow clone needs Delta). Flag
   external / non-Delta / streaming targets in `unresolved[]` — they need special handling.

## Flags (mark `nondeterministic=true`)
`current_timestamp()`/`now()`, `rand()`/`uuid()`, `collect_list`/`first` without an explicit sort,
non-deterministic UDFs, or `MERGE` whose matching depends on unstable ordering. If flagged, do NOT
auto-optimize — report it.

## Finish
Write `source_tables`, `target_tables` (+ `operation`), `nondeterministic`, `unresolved` into the
notebook's `opt_config` row. `audit_log(step="detect_tables", insight=<summary + any unresolved>)`.
**A human confirms the lists before sandbox-setup runs.**
