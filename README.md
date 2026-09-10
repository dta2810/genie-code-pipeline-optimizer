# Genie Code Pipeline Optimizer

A **governed, equivalence-gated optimizer** for slow Databricks pipelines, driven by **Genie
Code**. You give it a **job name**; it works through the job's notebooks, proposes optimizations,
and ships a v2 **only when it is proven to produce the same result as the baseline** (before ≡
after) *and* runs faster/cheaper. Every step is audited, including Genie's own natural-language
insight on *why*.

> Reuses the governed pattern of the Agentic Model Factory: skills + an append-only audit table +
> **hard gates that RAISE** + gated promotion + config-as-code. Here the "model" is an optimized
> notebook, and the "AUC guard" is the **equivalence + performance gate**.

## How to run it

Invoke the orchestrator skill on demand with `@` — no always-on project instructions needed:

```
@optimize-pipeline optimicemos el pipeline <job_name>
```

`optimize-pipeline` is self-contained (flow + human gates + audit contract + guardrails) and drives
the sub-skills. `.assistant_instructions.md` is deliberately **not** deployed, so the optimizer
engages only when you call it. Invoke from the `genie_code_optimizer` project folder so the skills
and `lib/` imports resolve.

## How it works

```
@optimize-pipeline <job_name>
   │  configure() → adopt THIS deployment's optimizer schema (auto-discovers; never double-provisions)
   │  bootstrap_from_job → notebooks in DAG order → rank hotspots (system.lakeflow timeline)
   │  SELECT which notebooks + order this run (gradual; you choose)         ← you pick
   │
   ├─ per selected notebook, in DAG order:
   │    detect-tables (Genie reads the code) → source/target tables, flag non-determinism
   │    perf-profile → the bottleneck + measured runtime
   │    propose the full applicable technique stack                          (GATE 1: you approve)
   │    optimize-notebook → write v2 <ntb>_genie_opt_<ts> (never edits the original)
   │    sandbox-setup → clone targets into sandbox_schema (WITH data) + pin inputs + remap writes
   │    run v1 & v2 on the dedicated cluster (not serverless)
   │    PROTOCOL gate  → assert_no_protocol_change (no reader/writer/feature bump)   ── HARD
   │    equivalence    → counts → fingerprint → EXCEPT ALL both ways (epsilon)       ── HARD
   │    perf-benchmark → median of N (advisory signal, not a block)
   │    security-review → secrets/injection/access/PII/out-of-sandbox/cost           ── HARD
   │    NO-AUDIT-NO-PROMOTE → assert_audited (opt_config row + every step recorded)   ── HARD
   │
   ├─ once ALL selected notebooks pass, for the whole job:
   │    flow-validate → run the OPTIMIZED job on shallow clones (never prod) and compare       ── HARD
   │                    every final target to the ORIGINAL run's recorded output — all
   │                    partitions (per-step equivalence alone does NOT prove this)
   │    show the audit trail + the flow-equivalence verdict                  (GATE 2: you approve)
   ▼
promote via the Jobs JSON — clone/repoint the job to v2 (original untouched; rollback = delete
the new job).  NEVER run the promoted (prod-pointing) job to "validate" — that writes to prod;
running it is a separate, human-gated production deploy.  [PR/DAB promotion is a future iteration.]
```

## The gates (what makes it safe)

| Gate | Type | Rule |
|---|---|---|
| Protocol | HARD | v2 must not bump Delta minReader/minWriter or add table features (no liquid clustering, deletion vectors, row tracking, generated cols, type widening). |
| Equivalence (step) | HARD | Per notebook: v2 output ≡ baseline — counts + per-column fingerprint + `EXCEPT ALL` both ways, within `epsilon`, on the FULL table (all partitions). The baseline is the reference truth. |
| Equivalence (flow) | HARD | Whole job: the entire optimized job, run on clones, reproduces the original run's recorded output on every final target. Per-step equivalence does NOT imply this — the DAG composes stages. |
| Security | HARD | v2 reviewed for secrets, injection, access/PII broadening, writes outside the sandbox, unsafe UDF/external calls, cost blowups. |
| Audit | HARD | No promotion unless the run is fully recorded (opt_config row + a succeeded row per required step). |
| Performance | **advisory** | Gain below `min_gain` is a **signal to you**, not an automatic block — a correctness/maintainability promotion is allowed and recorded as such. |

**Two levels of equivalence.** *Step* proves each notebook v2 ≡ v1 in isolation; *flow* proves the
whole optimized job lands on the same tables as the original — both are required before promotion,
because a per-step-green job can still be wrong once its stages compose (or once a rewrite behaves
differently at job scale). A green job run is **never** evidence of correctness; only the equivalence
assertion is. Equivalence proves correctness; performance is a separate reading. Prod is never at
risk: every clone and remapped write lands in the sandbox schema, MERGE/UPDATE targets are cloned
**with their data** (not empty), and inputs are pinned via time-travel so before/after read identical data.

## The optimization catalog is a starting set, not a cage

`optimization-catalog` ships 6 recipes (broadcast join, skew, clustering→Z-ORDER only, small-files,
de-UDF, incremental-MV), each with symptom → detection → before/after → equivalence risk → guard
notes. But the optimizer **may also use Genie Code's own skills** (`writing-sql`,
`table-optimization`, `data-modification`, `performance-tuning`) when they offer a better technique
— it names the source at GATE 1 and in the audit. Safety comes from the **gates**, not from limiting
where a technique comes from. The only hard prohibition, regardless of source, is a protocol/feature
bump.

## Skills (what each one does)

Each skill is a `SKILL.md` (Agent-Skills frontmatter) that describes *when/why*, and imports the
matching `lib/` module via its `scripts/` for the *how* — so behavior stays consistent and testable,
never reimplemented inline.

| Skill | Role | What's in it |
|---|---|---|
| `optimize-pipeline` | **entry point / orchestrator** | `@optimize-pipeline` — self-contained flow (bootstrap → select → per-notebook gates → whole-job flow-validate → promote). Owns the audit contract, the two HITL gates, and the guardrails below. |
| `detect-tables` | source/target discovery | Genie **reads** the notebook (no SQL parser) → resolves dynamic/widget table names, cross-checks UC lineage, flags non-determinism (`current_timestamp`/rand/unordered agg). HITL-confirmed. |
| `perf-profile` | bottleneck diagnosis | Root-causes the chosen notebook from the job JSON + `system.query.history` + Delta `DESCRIBE HISTORY` (full-rewrite-vs-delta-touch, spill, row amplification); proposes the full applicable technique stack up front. |
| `optimization-catalog` | technique recipes | 6 recipes (broadcast, skew, clustering→Z-ORDER, small-files, de-UDF, incremental-MV): symptom → detection → before/after → equivalence-risk tier → guard. A starting set, not a cage. |
| `optimize-notebook` | generate v2 | Writes `<ntb>_genie_opt_<ts>` (never edits the original). Enforces no protocol bump and Spark-Connect-safe rewrites — e.g. a partition reload uses `INSERT … REPLACE WHERE <keys>` with **literal** keys, never a bare `INSERT OVERWRITE` (which is a static full-table overwrite that drops other partitions). |
| `sandbox-setup` | isolation | Shallow-clone targets WITH data, pin inputs (time-travel), remap ALL writes to the sandbox schema, verify no prod ref remains; sampled-vs-full tier per operation. |
| `equivalence-check` | **step gate** (HARD) | Per notebook: counts → fingerprint → `EXCEPT ALL` both ways, on the FULL table, epsilon by risk tier. RAISES on divergence. |
| `flow-validate` | **flow gate** (HARD) | Whole job: replay the optimized job on clones from a real past run's inputs and compare each final target to that run's recorded output (or two-schema A/B). Catches composition/scale bugs a step gate can't. |
| `perf-benchmark` | perf reading (advisory) | Median of N on the dedicated cluster; separates equivalence (proof) from perf (signal); on serverless reports structural I/O, not contaminated wall-clock. |
| `security-review` | **late security gate** (HARD) | v2 reviewed for secrets, injection, access/PII broadening, out-of-sandbox writes, unsafe calls, cost blowups. Any finding blocks promotion. |

## Guardrails (what makes the guarantee hold)

The framework's promise — *ship only what's proven to produce the same result* — is only as good as
these invariants. Each one closes a specific way a wrong optimization could otherwise reach prod, so
they are enforced by the harness and the orchestrator, not left to judgment.

- **Validation never writes prod → you can safely point it at a live job.** Every run (per-step and
  whole-job) executes against sandbox clones; promotion only repoints the job definition, it runs
  nothing. This is what lets the optimizer work against a real pipeline without risk.
- **The gate is the *only* path to prod → correctness and deployment stay separate.** Running the
  promoted job (its tasks point at prod) is a deploy, not a check; validation happens on clones via
  `flow-validate`. Without this split, a "let's just run it and see" writes prod and bypasses the gate.
- **Deploy exactly what you validated → the gate actually means something.** Any edit to a v2 after it
  passed — even a hotfix to make it run — re-enters the full gate. Otherwise the gate certifies an
  artifact that isn't the one that ships.
- **Correctness comes from the assertion, never from a green run → silent data changes can't pass.**
  A job that runs green can still be wrong; equivalence is checked on the FULL table (all partitions),
  so a rewrite that quietly drops rows/partitions is caught instead of shipped.
- **The gate is never bent to pass → the proof stays honest.** `epsilon`/`min_gain` are never softened;
  non-deterministic notebooks are flagged and excluded, not optimized blind; and rewrites stay portable
  (no session `spark.conf.set(...)` on serverless — `CONFIG_NOT_AVAILABLE`; use `TBLPROPERTIES` /
  `REPLACE WHERE`).

## Compute (where the heavy work runs)

Control work (config, detect-tables, audit, equivalence on a bounded sample) runs on the serverless
Genie Code session. The **heavy** work — running the v1/v2 notebooks and the wall-clock benchmark —
is submitted to a **dedicated cluster** via `jobs runs submit` + `existing_cluster_id`, because
serverless cold-starts and loses cell state on autoscale (large rewrites time out) and its
wall-clock is contaminated by setup. `lib.compute.resolve_compute(cfg)` picks the cluster:

```
explicit override  >  opt_config.compute_cluster_id  >  the job's own compute  >  settings default  >  serverless
```

If it resolves to serverless, the run warns you and reports the **structural I/O signal** (rows/files
rewritten) instead of an unreliable wall-clock.

## Configuration

Two levels, both resolved at runtime — never hard-coded.

**1 · Where the optimizer lives** (`lib/settings.py`). Resolution order:

```
explicit configure(...) arg  >  runtime config file  >  auto-discovery  >  env var  >  default
```

| Setting | Env var | Default |
|---|---|---|
| `OPTIMIZER_CATALOG` | `PO_OPTIMIZER_CATALOG` | `main` |
| `OPTIMIZER_SCHEMA` | `PO_OPTIMIZER_SCHEMA` | `genie_optimizer` |
| `SANDBOX_SCHEMA` | `PO_SANDBOX_SCHEMA` | `pipeline_opt_sandbox` |
| `WORKSPACE_HOME` | `PO_WORKSPACE_HOME` | `/Workspace/Users/<you>/genie_code_optimizer` |
| `COMPUTE_CLUSTER_ID` | `PO_COMPUTE_CLUSTER_ID` | *(empty → serverless)* |

Env vars don't reach the Databricks runtime, so `provision` writes a small **runtime config file**
at `/Workspace/Users/{user}/.genie_optimizer.json` (outside the bundle sync root, so redeploys never
delete it); `settings.configure(spark=spark)` reads it. If it's absent, `configure()`
**auto-discovers** an existing optimizer schema (by scanning for its `opt_config` table) and adopts
it instead of provisioning a duplicate.

**2 · Per-job / per-notebook** (`opt_config` table, seeded by `bootstrap_from_job`):

| Column | Default | Purpose |
|---|---|---|
| `epsilon` | `1e-6` | equivalence float tolerance |
| `min_gain` | `0.20` | advisory perf threshold |
| `benchmark_runs` | `3` | runs for the median |
| `validation_tier` | `sampled` | sample big targets so equivalence stays serverless-friendly |
| `compute_cluster_id` | *(inherits the setting)* | governed dedicated compute, per job |
| `source_tables` / `target_tables` | *(empty)* | filled by `detect-tables` reading the notebook |

The sandbox is **schema-based**, inside each target's own catalog: a clone of
`cat.schema.table` lands at `cat.<SANDBOX_SCHEMA>.<schema>__<table>` (`settings.sandbox_fqn`) — no
CREATE CATALOG privilege, no cross-catalog collisions.

## Deploy (DABs)

Locations are bundle variables, overridden per target in `databricks.yml` — one target per
environment, no code edits.

```bash
databricks bundle validate --strict -t latam
databricks bundle deploy -t latam                    # syncs skills + lib + sql + deploy to WORKSPACE_HOME
databricks bundle run provision_optimizer -t latam   # creates schema + tables + views + get_opt_config
```

`deploy` lands the code at `WORKSPACE_HOME` (no `/files` subdir) so Genie Code auto-loads
`.assistant/skills` and the `lib/` imports resolve; `provision_optimizer` runs `deploy/00_deploy.py`
(idempotent — `CREATE ... IF NOT EXISTS` / `OR REPLACE`, and it ALTERs new columns onto an existing
`opt_config`). A new environment = copy the `latam` target block and set its `optimizer_catalog` /
`optimizer_schema` / `sandbox_schema` / `compute_cluster_id` / `profile`.

**Incremental code-only updates — prefer `sync_assets.sh`.** For pushing edited skills / `lib` to an
existing deployment without re-provisioning, use `import-dir --overwrite`, which only adds/overwrites
the dirs it copies:

```bash
source deploy/env.sh && bash deploy/sync_assets.sh   # imports .assistant, lib, deploy, sql to WORKSPACE_HOME
```

Use it instead of `bundle deploy` when the optimizer has already produced runtime artifacts under
`WORKSPACE_HOME` (the per-job `jobs/<job>/optimized/*` and `validation/*` notebooks a promoted job
points at): `bundle deploy` mirror-syncs `file_path` and can **prune** workspace files that aren't in
the repo, whereas `import-dir --overwrite` never deletes. Re-run `provision_optimizer` only when
`sql/` or the `opt_config` schema changed.

## Repo layout

```
genie-code-pipeline-optimizer/
├── .assistant/skills/          # Genie Code auto-loaded skills (Agent Skills spec)
│   ├── optimize-pipeline/          ← ENTRY POINT: @optimize-pipeline (orchestrator, self-contained)
│   ├── detect-tables/              # Genie reads the notebook → source/target tables (no parser)
│   ├── perf-profile/               # deep root-cause diagnosis of the chosen notebook
│   ├── optimization-catalog/       # reference recipes (starting set, not a cage)
│   ├── optimize-notebook/          # generate v2 <ntb>_genie_opt_<ts> (HITL)
│   ├── sandbox-setup/              # clone targets + pin inputs + remap writes
│   ├── equivalence-check/          # counts → fingerprint → EXCEPT ALL, step-by-step (per notebook)
│   ├── flow-validate/              # whole-job equivalence: orig job DAG vs opt job DAG, one snapshot
│   ├── perf-benchmark/             # fair perf measurement (median of N, advisory)
│   └── security-review/            # late security gate before promotion
├── lib/                        # importable harness — skills import, never reimplement:
│   ├── settings.py                 # runtime config resolution + optimizer_fqn / sandbox_fqn
│   ├── config.py                   # bootstrap_from_job, select_notebooks, sync_config
│   ├── perf.py                     # rank_notebooks (hotspot ranking) + perf gate
│   ├── compute.py                  # resolve_compute + run_notebook + benchmark_on_cluster
│   ├── sandbox.py                  # clone_targets (full/sampled) + remap_writes
│   ├── equivalence.py              # assert_equivalent + assert_no_protocol_change
│   ├── flow.py                     # whole-job (flow) equivalence: two-schema clone + run both DAGs + compare
│   ├── audit.py                    # audit_log + assert_audited (no-audit-no-promote)
│   └── promote.py                  # promote_notebook via the Jobs JSON (new job / in place)
├── sql/                        # tables.sql · config_function.sql · governance_views.sql
├── deploy/                     # provision.py (logic) + 00_deploy.py (notebook entry)
├── resources/                  # provision_optimizer.job.yml (DABs job)
├── databricks.yml              # bundle vars + per-target overrides
└── docs/                       # architecture, design notes
```

## Status

Harness built and **deployed on a live workspace**: the full flow runs end-to-end (detect → profile →
GATE 1 → v2 → sandbox → protocol + step-equivalence + security + audit → **flow-validate** → GATE 2 →
promote). Both equivalence levels are in: **step** (per notebook) and **flow** (whole job, replay
against a real past run's recorded output, or two-schema A/B) — the flow harness is smoke-tested
end-to-end (catches a static-overwrite that drops partitions, passes the partition-scoped fix).
Governed dedicated compute and multi-notebook selection are wired. Next: PR/DAB promotion (Loop 2,
CI re-runs the same gate) and the Optimization Command Center dashboard over the governance views.
See `docs/architecture.md` for the two-loops design and the project tracker for the backlog.
