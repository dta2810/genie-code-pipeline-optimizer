---
name: flow-validate
description: Prove the WHOLE optimized job produces the same result as the WHOLE original job — job-level (flow) equivalence, not just per-notebook. Two modes. REPLAY (preferred, `run_replay`): pin inputs to when the original pipeline really ran (Delta time-travel), run ONLY the optimized job, and compare to that run's RECORDED output — no synthetic data, no re-running the original. TWO-SCHEMA (`run`): when no past run is anchorable, clone every table into two parallel schemas from one snapshot and run BOTH job DAGs from identical inputs. Both compare every final target with the equivalence ladder and audit a flow_equivalence verdict. Run AFTER all notebooks are step-validated.
---

# flow-validate

Import `scripts/run_flow_validate.py` — do NOT reimplement inline. Wraps `lib.flow`.

**Why:** `equivalence-check` proves each notebook v2 ≡ v1 on independently pinned/cloned inputs.
That is necessary but NOT sufficient — it never proves the composed DAG (whole original job) and the
composed DAG (whole optimized job) land on the same final tables. A green optimized run is not proof.
flow-validate is the flow-level gate; run it once all notebooks are step-validated.

## Mode A — REPLAY against a real past run (preferred)

`run_replay(...)`. Answers "if the pipeline ran N days ago, does the optimized job give the same
end-to-end result?" using the **real data as it was**, and the original run's **recorded output** as
ground truth — the most faithful test, and it never re-runs the original.

1. **Anchor a real past original-pattern run** from Delta history: `anchor_before` = the timestamp it
   started (inputs — sources AND each target's pre-run state — are pinned there via `TIMESTAMP AS OF`);
   `anchor_after` = a timestamp just after it finished (its outputs read there = ground truth).
2. **Materialize the pinned inputs** into a `<src_schema>_replay` schema (CTAS snapshot).
3. **Run ONLY the optimized job** into that schema (widget-param swap, dedicated compute).
4. **Compare each final target** to the original run's recorded output (`prod TIMESTAMP AS OF anchor_after`)
   with the full ladder; audit `flow_equivalence`.
   Requires the anchor to still be time-travelable (not VACUUMed) — check first.

## Mode B — TWO-SCHEMA (when no past run is anchorable)

**Inputs:** the original job id, the optimized job id, the catalog + source schema the job's tables
live in, the full table list (sources + targets), the final targets to compare, the dedicated
`cluster_id` (never serverless — the original job's baseline is heavy), and optionally
`sample_percent` + `sample_only` (sample a huge driver table; same seed keeps _orig/_opt identical).

`run(...)`. Use when there is no clean past run to anchor (e.g. a brand-new optimized job).

1. **One snapshot → two schemas.** Clone every table into `<src_schema>_flow_orig` and
   `<src_schema>_flow_opt`. Sampled tables use `TABLESAMPLE ... REPEATABLE(seed)` (same seed ⇒ both
   sides identical); every other table is a full `SHALLOW CLONE` (preserves partitioning — required
   for partition-scoped reloads). Never DROP.
2. **Run each whole DAG from identical inputs.** Rebuild each job's task list, inject
   `base_parameters {catalog, schema}` into EVERY notebook task (the original job may leave some
   empty → they would default to prod), pin `existing_cluster_id`, submit via `runs/submit` (ephemeral
   — the stored jobs are never mutated; prod tables are never written).
3. **Compare every final target** `_orig.T` vs `_opt.T` with the full ladder (counts → fingerprint →
   `EXCEPT ALL` both ways, epsilon by `risk_tier`). Collect a per-table verdict — don't stop at the
   first divergence, so the report is complete.
4. **Audit** `flow_equivalence` (`succeeded`/`failed`) with the per-target map + an NL insight.

**Safety:** prod-safe (writes only ever hit the sandbox schemas via the param swap). Non-deterministic
notebooks make flow equivalence undefined — the same rule as step equivalence; flag and refuse.

**HITL:** a clean equivalence FAIL does not RAISE — it returns a verdict for a human to decide
promote vs rollback. Operational failures (a job run that did not SUCCEED) RAISE.

**Scale:** full-scale on the dedicated cluster is the real proof. A sampled start (same seed both
sides) validates the mechanism and the logic cheaply, but is a logical proof, not a volume proof —
label it as such.

**Cleanup (offer it, don't auto-run).** The verdict returns `replay_schema` (the `<src_schema>_replay`
/ `_flow_orig` / `_flow_opt` sandbox schema of shallow clones). After showing the verdict, **suggest**
dropping it: `flow.cleanup_replay(spark, catalog=..., schema=verdict["replay_schema"])`. It's safe
(shallow clones → drops metadata, never prod data), but leave it to the user so they can inspect the
clones first — do NOT drop automatically.
