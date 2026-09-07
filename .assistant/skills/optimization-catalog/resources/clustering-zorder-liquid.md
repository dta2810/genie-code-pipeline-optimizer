# Clustering (Z-order / Liquid)

## Symptom
Full-table or near-full scans on queries that filter/join on a few columns; poor data skipping.
Or fragile manual `PARTITIONED BY` that over-partitions and creates small files.

## Detection signal
- Query profile: scanned bytes/files ~= whole table despite selective filters.
- High file count per read; partition folders with tiny files.
- Frequent filter/join predicates on the same 1–4 columns.

## Recipe — Z-ORDER only (liquid clustering is FORBIDDEN here)

Use **Z-ORDER during OPTIMIZE** — it improves data skipping WITHOUT bumping the Delta protocol:

```sql
OPTIMIZE t ZORDER BY (customer_id, event_date);
```

Z-order on the columns that actually appear in filters/joins (from perf-profile), not on high-null
or monotonic columns.

**Do NOT use Liquid Clustering (`CLUSTER BY` / `CLUSTER BY AUTO`)** — it adds the `clustering` table
feature and bumps `minWriterVersion` to 7, changing the downstream contract. Forbidden by the
catalog's protocol rule; `assert_no_protocol_change` will block it. Same for deletion vectors and
row tracking. If the table cannot benefit from Z-order without those, skip clustering entirely.

## Equivalence risk
**Zero-risk rewrite.** Z-order only reorganizes physical layout — table contents are unchanged.
This is a table-maintenance change, not a query rewrite.

## Guard notes
- This changes the *table*, not the notebook logic — the equivalence gate is trivially satisfied,
  but confirm the run targets the sandbox clone.
- **Protocol:** Z-order does not bump reader/writer version; liquid clustering does — never use it.
- Prove the win with a **filtered** read benchmark (`WHERE` on the z-order cols), not a full scan —
  a full scan can't show data-skipping. The OPTIMIZE itself is a one-time cost.
