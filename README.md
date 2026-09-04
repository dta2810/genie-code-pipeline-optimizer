# Pipeline Optimization Factory — Genie Code

A **governed, equivalence-gated factory** for optimizing slow Databricks pipelines with
**Genie Code**. You give it a **job name**; it works through the job's notebooks, proposes
optimizations, and ships a v2 **only when it is proven to produce the same result as the
baseline** (before ≡ after) *and* runs faster/cheaper. Every step is audited, including
Genie's own natural-language insights.

> Built on the same governed-factory pattern as the Agentic Model Factory: skills +
> `.assistant_instructions.md` audit contract + an append-only audit table + a **hard gate
> that RAISEs** + gated challenger/champion promotion. Here the "model" is an optimized
> notebook and the "AUC guard" is the **equivalence + performance gate**.

## How to run it

Invoke the orchestrator skill on demand with `@` — no always-on project instructions needed:

```
@optimize-pipeline optimicemos el pipeline <job_name>
```

`optimize-pipeline` is self-contained (flow + human gates + audit contract + guardrails) and
drives the sub-skills. We deliberately do **not** deploy `.assistant_instructions.md` to the
workspace, so the optimizer engages only when you call it.

## How it works

```
@optimize-pipeline <job_name> ─▶ Jobs API get job (JSON) ─▶ auto-fill opt_config
   │
   ├─ per notebook, in DAG order:
   │    read code ─▶ detect source/target tables ─▶ flag non-determinism
   │    ─▶ propose suggestions ─▶ HUMAN approves            (GATE 1)
   │    ─▶ generate v2 notebook  <ntb>_genie_opt_<ts>  (dedicated folder)
   │    ─▶ SANDBOX: shallow-clone targets to sandbox_catalog + pin inputs (time-travel)
   │    ─▶ run v2 end-to-end against clones
   │    ─▶ VALIDATE step-by-step: counts ─▶ column fingerprint ─▶ EXCEPT ALL (both ways)
   │       + perf (runtime / DBU, median of N)
   │    ─▶ AUDIT every step + Genie insight
   │    ─▶ HUMAN approves promotion                         (GATE 2)
   ▼
promote via PR / DAB back into the job (CI re-runs the same gate)
```

## Safety model (why prod is never at risk)

- **Sandbox by catalog** — every clone and every remapped write lands in `sandbox_catalog`.
  One catalog switch, not per-table remapping.
- **Shallow CLONE, not empty tables** — MERGE/UPDATE targets are shallow-cloned *with their
  current data* (zero-copy), so the operation behaves exactly like production. An empty table
  would give a wrong merge result.
- **Pinned inputs** — baseline and candidate read the exact same input via Delta time-travel
  (`VERSION AS OF`) or a clone. Otherwise "before vs after" compares different data.
- **Non-determinism check first** — timestamps, random, unordered aggregation, non-deterministic
  UDFs make equivalence undefined; such notebooks are flagged, not silently "optimized".

## Equivalence harness (before ≡ after)

Proves the candidate is equivalent **to the baseline** (the baseline is the reference truth),
step by step, cheapest check first:

1. **Counts** — total and per-partition.
2. **Column fingerprint** — sum / min / max / count-distinct / null-count per column.
3. **Hard proof** — `A EXCEPT ALL B` and `B EXCEPT ALL A`, both empty (order- and
   duplicate-safe), with epsilon tolerance for `DECIMAL`/`DOUBLE`.
4. **Step-by-step** — each intermediate operation is materialized and compared, so divergence
   is *localized*, not just detected.

## Repo layout

```
genie-code-pipeline-optimizer/
├── .assistant/
│   └── skills/                 # Genie Code auto-loaded skills (Agent Skills spec)
│       ├── optimize-pipeline/      # ← ENTRY POINT: @optimize-pipeline (orchestrator, self-contained)
│       ├── perf-profile/           # diagnose the bottleneck notebook/step
│       ├── detect-tables/          # Genie Code reads the notebook → source/target tables (no parser)
│       ├── sandbox-setup/          # clone targets + pin inputs + remap writes
│       ├── equivalence-check/      # counts → fingerprint → EXCEPT ALL, step-by-step
│       ├── perf-benchmark/         # fair perf measurement (median of N)
│       └── optimize-notebook/      # generate v2 <ntb>_genie_opt_<ts>  (HITL)
├── .assistant_instructions.md  # audit contract + import-don't-reimplement directive
├── config/
│   └── example_job.yaml        # opt_config shape (auto-seeded from the job JSON)
├── lib/                        # importable harness (settings/audit/config/sandbox/equivalence/perf)
├── sql/                        # tables.sql · config_function.sql · governance_views.sql
├── deploy/
│   └── 00_deploy.py            # provisions the UC assets (schema/tables/function/views)
└── docs/
    └── architecture.md
```

Each phase skill has `scripts/` that wrap the `lib/` harness (imported, not reimplemented).

## Deploy skills + instructions

All assets live under one configurable workspace folder — `PO_WORKSPACE_HOME`
(e.g. `/Workspace/Users/<you>/genie_code_optimizer`). Skills follow the **Agent Skills spec**
(frontmatter `name`/`description`), co-deployable with the official
[databricks/databricks-agent-skills](https://github.com/databricks/databricks-agent-skills).

```bash
cp deploy/env.example.sh deploy/env.sh     # fill in PO_WORKSPACE_HOME + factory schema + profile
source deploy/env.sh
bash deploy/sync_assets.sh                  # push skills + lib + deploy + sql to $PO_WORKSPACE_HOME
# then run deploy/00_deploy.py in the workspace to provision the UC assets
```

## Status

Scaffold. See the task tracker (`sa-workspace`) for the build checklist. Skills are stubs
(`SKILL.md` intent only) pending the harness implementation.
