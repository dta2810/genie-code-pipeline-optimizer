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
| Isolation | `sandbox-setup`: shallow CLONE of targets (with data), time-travel-pinned inputs, writes remapped to `sandbox_catalog` |
| Correctness | `equivalence-check`: counts → column fingerprint → `EXCEPT ALL` both ways, step-by-step, epsilon tolerance |
| Performance | `perf-benchmark`: same compute, cache control, median of N, DBU/shuffle/spill |
| Promotion | champion (v1) / challenger (v2); promote only if equivalent AND faster by `min_gain`; human GATE 2 |
| Governance | `optimization_audit` (append-only) + `insight` + Optimization Command Center dashboard |

## Safety invariants

1. Production tables are never written — every write is remapped to `sandbox_catalog`.
2. MERGE/UPDATE targets are cloned **with data** (shallow clone), never empty.
3. Baseline and candidate read **identical pinned inputs**.
4. Non-deterministic notebooks are flagged and excluded, not silently optimized.
5. Equivalence tolerance is never softened to force a pass.
