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
