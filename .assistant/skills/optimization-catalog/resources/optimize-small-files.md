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

Prevent new small files at write time:

```python
# optimized writes + auto-compaction on the target table
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

```sql
ALTER TABLE t SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
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
