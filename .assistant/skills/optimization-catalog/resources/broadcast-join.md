# Broadcast join

## Symptom
Large shuffle stage on a join where one side is small. In the query profile: a
`SortMergeJoin`/shuffle-heavy join whose smaller input is under ~30 MB.

## Detection signal
- Spark SQL metrics: high shuffle read/write on the join stage.
- One join input's scanned bytes << the other (dimension vs fact).
- `system.query.history` slow-query rows (diagnostic-queries.sql #1) landing on a join.

## Recipe
Broadcast the small side so the large side is never shuffled.

```python
# after
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "join_key")
```

```sql
-- after (SQL hint)
SELECT /*+ BROADCAST(small_table) */ ...
FROM large_table JOIN small_table USING (join_key);
```

Prefer letting AQE auto-broadcast (`spark.sql.adaptive.autoBroadcastJoinThreshold`) when the
small side is reliably under the threshold; use the explicit hint when stats mislead the optimizer.

## Equivalence risk
**Zero-risk rewrite.** Join semantics are unchanged — only the physical strategy differs. Output
rows and columns are identical by construction.

## Guard notes
- Confirm the broadcast side truly fits in memory (driver + executor) — an oversized broadcast OOMs
  instead of speeding up. If the "small" side can grow, keep it AQE-driven, not a hard hint.
- Gate: `EXCEPT ALL` both ways should be empty on the first pass; treat any divergence as a bug in
  detect-tables (wrong join key), not a tolerance issue.
