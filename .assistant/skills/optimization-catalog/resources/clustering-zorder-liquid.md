# Clustering (Z-order / Liquid)

## Symptom
Full-table or near-full scans on queries that filter/join on a few columns; poor data skipping.
Or fragile manual `PARTITIONED BY` that over-partitions and creates small files.

## Detection signal
- Query profile: scanned bytes/files ~= whole table despite selective filters.
- High file count per read; partition folders with tiny files.
- Frequent filter/join predicates on the same 1–4 columns.

## Recipe
Prefer **Liquid Clustering** for new/managed tables (no partition fragility, adapts over time):

```sql
-- new table
CREATE TABLE t (...) CLUSTER BY (customer_id, event_date);

-- existing table
ALTER TABLE t CLUSTER BY (customer_id, event_date);
OPTIMIZE t;   -- clusters existing data
```

For tables that cannot adopt liquid yet, Z-order during OPTIMIZE:

```sql
OPTIMIZE t ZORDER BY (customer_id, event_date);
```

Cluster on the columns that actually appear in filters/joins (from perf-profile), not on high-null
or monotonic columns.

## Equivalence risk
**Zero-risk rewrite.** Clustering/Z-order only reorganizes physical layout — table contents are
unchanged. This is a table-maintenance change, not a query rewrite.

## Guard notes
- This optimization changes the *table*, not the notebook logic — the equivalence gate on the
  pipeline output is trivially satisfied, but confirm the run still targets the sandbox clone.
- Liquid Clustering and explicit `PARTITIONED BY` are mutually exclusive — do not mix.
- Measure the win on the *read* side (downstream scans), not just the OPTIMIZE runtime, which is a
  one-time cost.
