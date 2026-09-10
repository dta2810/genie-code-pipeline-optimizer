# Small files

## Symptom
Long list/scan time driven by file count, not data size — many tiny files per table. Common after
frequent small appends or over-partitioning.

## Detection signal
- File count per read is high relative to table size (many files << target ~128 MB–1 GB each).
- `DESCRIBE DETAIL t` → `numFiles` large vs `sizeInBytes`.
- Scan time dominated by task setup, not bytes read.

## Recipe
Compact existing files and prevent recurrence:

```sql
-- one-time compaction
OPTIMIZE t;                       -- bin-packs small files
-- (add ZORDER BY (...) only if clustering is also warranted)
```

Prevent new small files at write time. **On serverless / Spark Connect prefer the table
`TBLPROPERTIES` form — `spark.conf.set(...)` for a session conf raises `CONFIG_NOT_AVAILABLE`
and fails the task.** Set it once on the target table instead:

```sql
ALTER TABLE t SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
```

Session-conf form (classic compute only — do NOT emit in a serverless job notebook):

```python
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

Anti-pattern to remove: `.repartition(N)` before write (unnecessary full shuffle) — use
`.coalesce(N)` or rely on optimized writes.

## Equivalence risk
**Zero-risk rewrite.** Compaction and optimized writes change file layout only; row/column content
is identical.

## Guard notes
- OPTIMIZE is a one-time cost — measure the win on downstream reads, not on the OPTIMIZE step.
- If you also drop a `.repartition()`, confirm output ordering was never relied on (Spark output
  order is not guaranteed either way; the `EXCEPT ALL` gate is order-insensitive, which is correct
  here).
