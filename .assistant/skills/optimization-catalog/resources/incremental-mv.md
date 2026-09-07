# Incremental MV

Turn an expensive gold/aggregate that is rebuilt in full every run into an incrementally-maintained
result — either a Materialized View or an incremental MERGE. Build-new (adapts the
`databricks-metric-views` / MV concept). See [[reference_mv_incremental_refresh]] for the hard
gotchas.

## Symptom
A gold step recomputes a full aggregate/join over all history on every run, though only recent
partitions changed. Runtime scales with total data, not with new data.

## Detection signal
- The notebook does `CREATE OR REPLACE TABLE gold AS SELECT ... GROUP BY ...` (full overwrite) or an
  `INSERT OVERWRITE` of the whole aggregate.
- Source tables grow monotonically; the expensive stage's input bytes ~= whole table each run.

## Recipe
Two options — pick by how the aggregate is consumed:

**A. Materialized View** (let Databricks maintain it):
```sql
CREATE MATERIALIZED VIEW gold_agg AS
SELECT k, SUM(x) AS total, COUNT(*) AS n
FROM silver GROUP BY k;
```
Incremental refresh only triggers when the source change is compatible. Per
[[reference_mv_incremental_refresh]]: the **source must land via MERGE/UPDATE, not full overwrite**;
DECIMAL / staged-MV / serverless caveats apply; `EXPLAIN` ≠ runtime — confirm incremental actually
fired via the event log, not the plan.

**B. Incremental MERGE** (explicit control): read only the changed window (watermark / date bound)
and `MERGE INTO gold` on the key, updating aggregates. Requires a reliable change boundary.

## Equivalence risk
**Refresh-semantics change — the trickiest gate.** The steady-state table must equal a full
recompute over the same inputs. Prove it by running, in the sandbox over pinned inputs:
1. v1 full recompute → `gold_full`
2. v2 incremental (seed + apply the changed window) → `gold_incr`
3. `EXCEPT ALL` both ways between `gold_full` and `gold_incr` must be empty.

Watch aggregate correctness across the boundary: late-arriving data, re-stated partitions, and
non-additive aggregates (`COUNT(DISTINCT)`, `AVG` without carrying counts) are the usual divergence
sources.

## Guard notes
- If the source is a full overwrite, MV incremental refresh will silently fall back to full
  recompute (no win) — fix the source write path first or the optimization is a no-op.
- Non-additive aggregates need a decomposition (carry SUM + COUNT for AVG) — flag if the aggregate
  cannot be maintained incrementally.
- Confirm the incremental path actually fired (event log), and measure the win on an incremental run
  (small change), not on the initial seed.
