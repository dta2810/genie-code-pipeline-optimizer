# Architecture

## Two loops

- **Loop 1 — optimize + prove (Genie Code + HITL).** Diagnose → propose → (human GATE 1) →
  generate v2 → run in sandbox → equivalence + perf gates → audit. Authoring only.
- **Loop 2 — deploy (CI/CD).** Promote the proven v2 back into the job via PR/DAB; CI re-runs
  the same equivalence + perf gate. "Deploy code, not artifacts."

## Components

| Component | Implementation |
|-----------|----------------|
| Entry | `job_name` → Jobs API get job (JSON) → auto-seed `opt_config` |
| Diagnosis | `perf-profile` skill over job JSON + `system.query.history` / `system.billing` + query profile |
| Candidate | `optimize-notebook` → `<ntb>_genie_opt_<ts>` in a dedicated folder |
| Isolation | `sandbox-setup`: shallow CLONE of targets (with data), time-travel-pinned inputs, writes remapped to a dedicated `sandbox_schema` in the target's own catalog |
| Correctness | `equivalence-check`: counts → column fingerprint → `EXCEPT ALL` both ways, step-by-step, epsilon tolerance |
| Performance | `perf-benchmark`: same compute, cache control, median of N, DBU/shuffle/spill |
| Security | `security-review`: late gate on v2 — secrets, injection, access/PII broadening, sandbox containment, unsafe calls, cost blowups |
| Promotion | champion (v1) / challenger (v2); promote only if equivalent AND faster by `min_gain`; human GATE 2 |
| Governance | `optimization_audit` (append-only) + `insight` + Optimization Command Center dashboard |

## Safety invariants

1. Production tables are never written — every write is remapped to the dedicated `sandbox_schema`.
2. MERGE/UPDATE targets are cloned **with data** (shallow clone), never empty.
3. Baseline and candidate read **identical pinned inputs**.
4. Non-deterministic notebooks are flagged and excluded, not silently optimized.
5. Equivalence tolerance is never softened to force a pass.
6. **Serverless / Spark Connect**: a v2 never sets a session SQL conf (`spark.conf.set(...)` →
   `CONFIG_NOT_AVAILABLE`). Delta+UC behaviors that a session conf would toggle (dynamic partition
   overwrite, optimizeWrite/autoCompact) are the default or set via `TBLPROPERTIES` instead.

## Two levels of equivalence

- **Step (per-notebook)** — *implemented.* For each notebook, v2 output ≡ v1 output on cloned targets
  with pinned inputs (counts → fingerprint → `EXCEPT ALL`). Necessary, and the promotion gate.
- **Flow (whole-job)** — *implemented (`lib/flow.py` + the `flow-validate` skill); pending its first run
  against a real workload.* Step equivalence proves each stage in isolation; it does not by itself prove
  the whole original job's final tables equal the whole optimized job's final tables (the DAG composes
  stages, and each step was validated on independent clones). Two modes:
  - **Replay (preferred)** — answers "if the pipeline ran N days ago, does the optimized job give the same
    end-to-end result?" using the real data as it was. Pin the inputs (sources + each target's pre-run
    state) to when a real past original run started (Delta time-travel), run ONLY the optimized job on
    them, and compare each final target to that run's RECORDED output. No synthetic data, no re-running the
    original; requires the anchor to still be time-travelable (not VACUUMed).
  - **Two-schema** — when no past run is anchorable: clone every table into two parallel sandbox schemas
    from one snapshot and run BOTH job DAGs from identical inputs (widget-param schema swap), then compare.

  Prod-safe: the jobs are re-pointed to sandbox schemas, so prod tables are never written. Until this gate
  passes on a real workload, the optimizer claims **step** equivalence only.

## The gates

| Gate | Type | Rule |
|---|---|---|
| Protocol | hard | v2 must not bump Delta minReader/minWriter or add table features (liquid clustering, deletion vectors, row tracking, generated columns, type widening). |
| Equivalence (step) | hard | Per notebook: v2 output equals the baseline — counts, per-column fingerprint, and `EXCEPT ALL` both ways, within `epsilon`, on the full table. The baseline is the reference truth. |
| Equivalence (flow) | hard | Whole job: the entire optimized job, run on clones, reproduces the original run's output on every final table. Step equivalence does not imply this — the DAG composes stages. |
| Security | hard | The v2 is reviewed for secrets, injection, access or PII broadening, out-of-sandbox writes, unsafe calls, and cost blowups. |
| Audit | hard | No promotion unless the run is fully recorded — an `opt_config` row and a succeeded row per required step. |
| Performance | advisory | A gain below `min_gain` is a signal to you, not an automatic block. A correctness or maintainability promotion is allowed, and recorded as such. |

Equivalence proves correctness; performance is a separate reading.

## Guardrails — what makes the guarantee hold

The promise is *ship only what's proven to produce the same result*, and it is only as good as these invariants.
Each one closes a specific way a wrong optimization could otherwise reach production, so they are enforced by the harness and the orchestrator, not left to judgment.

**Validation never writes production, so you can safely point the optimizer at a live job.**
Every run — per-step and whole-job — executes against sandbox clones, and promotion only repoints the job definition; it runs nothing.

**The gate is the only path to production, so correctness and deployment stay separate.**
Running the promoted job (its tasks point at production) is a deploy, not a check; validation happens on clones through `flow-validate`.
Without this split, a "let's just run it and see" writes production and bypasses the gate.

**You deploy exactly what you validated, so the gate means something.**
Any edit to a v2 after it passed — even a hotfix to make it run — re-enters the full gate.
Otherwise the gate certifies an artifact that isn't the one that ships.

**Correctness comes from the assertion, never from a green run, so silent data changes cannot pass.**
A job that runs green can still be wrong; equivalence is checked on the full table, so a rewrite that quietly drops rows or partitions is caught instead of shipped.

**The gate is never bent to pass, so the proof stays honest.**
`epsilon` and `min_gain` are never softened; non-deterministic notebooks are flagged and excluded, not optimized blind; and rewrites stay portable — no session `spark.conf.set(...)` on serverless (it raises `CONFIG_NOT_AVAILABLE`); use `TBLPROPERTIES` or `REPLACE WHERE` instead.

## Roadmap

- **Loop 2 promotion via PR/DAB** — promote the proven v2 back into the job as a pull request / bundle change, with CI re-running the same equivalence + performance gate. "Deploy code, not artifacts."
- **Optimization Command Center** — a governance dashboard over the audit views (`optimization_audit` + the governance views) showing runs, gate outcomes, and gains across jobs.
