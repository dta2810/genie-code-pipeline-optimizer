# Deploy

Deploy has **two halves**, and they run in different places:

1. **Get the code into the workspace** — the `.assistant/skills`, `lib/`, `sql/`, `deploy/` files must physically live at your `WORKSPACE_HOME` so Genie Code auto-loads the skills and the `lib/` imports resolve. This is a **file sync from outside** the workspace (CLI or a Git folder). It is a **one-time bootstrap** (re-run only when you edit skills or `lib`).

2. **Provision the optimizer UC assets** — schema, `opt_config`, `optimization_audit`, `get_opt_config`, governance views, plus the runtime config file. This runs **entirely inside Databricks** via `spark`, driven by **`config/optimizer.yaml`**. Launch it manually or from Genie Code — no DABs, no CLI.

## Prerequisites

- The **Databricks CLI** on your PATH (`databricks --version`) with an authenticated profile (`databricks auth login`, lands in `~/.databrickscfg`) — needed only for the one-time code bootstrap in step 1 (a Git folder avoids even this).
- Permission to create a schema in your optimizer catalog, write under your `WORKSPACE_HOME`, and submit job runs on the dedicated cluster.

## 1. Bootstrap the code into the workspace (one-time)

Pick one. Both land the code at `WORKSPACE_HOME` with **no `/files` subdir** so Genie Code finds `.assistant/skills` and `lib/` imports resolve.

**CLI `import-dir`** (cross-platform, no bash):

```
databricks workspace import-dir .assistant  <WORKSPACE_HOME>/.assistant --overwrite --profile <profile>
databricks workspace import-dir lib          <WORKSPACE_HOME>/lib        --overwrite --profile <profile>
databricks workspace import-dir deploy       <WORKSPACE_HOME>/deploy     --overwrite --profile <profile>
databricks workspace import-dir sql          <WORKSPACE_HOME>/sql        --overwrite --profile <profile>
```

`import-dir --overwrite` adds and overwrites only the dirs it copies and **never prunes**, so it is safe over the per-job runtime artifacts (`jobs/<job>/optimized|validation|driver`). Re-run it whenever you edit skills or `lib`.

**Or a Git folder** — add the repo as a workspace Git folder (needs a remote); `git pull` in the folder updates the code with no CLI. Keep `WORKSPACE_HOME` (where runtime artifacts are written) separate from the Git folder so generated notebooks don't dirty the working tree.

## 2. Provision the optimizer UC assets — `config/optimizer.yaml` (default)

Edit **`config/optimizer.yaml`** (catalog, schema, sandbox schema, cluster, workspace home). Then launch it either way — both call `provision()` via `spark`, entirely inside Databricks:

- **Manual** — open `deploy/00_deploy.py` and **Run All**. The widgets pre-fill from `config/optimizer.yaml`; override in the widgets if you want.
- **From Genie Code** —

  ```python
  from provision import deploy_from_yaml
  deploy_from_yaml(spark)
  ```

Idempotent (`CREATE … IF NOT EXISTS` / `OR REPLACE`, and it ALTERs new columns onto an existing `opt_config`). Run it on first deploy or when `sql/` or the `opt_config` schema changed. It also writes the runtime config file `~/.genie_optimizer.json` so the harness needs no process env.

Verify:

```sql
SELECT table_name, table_type
FROM <catalog>.information_schema.tables
WHERE table_schema = '<optimizer_schema>' ORDER BY table_type, table_name;
```

## Alternatives (optional): DABs and sync scripts

The DABs bundle and the shell/Python sync scripts still ship in the repo, but they are **optional** — the `config/optimizer.yaml` path above is the default.

**DABs** — pure CLI, cross-platform. Locations are bundle variables in `databricks.yml`, overridden per target (mirror `config/optimizer.yaml`).

```bash
databricks bundle validate --strict -t <target>
databricks bundle deploy -t <target>                    # syncs code to WORKSPACE_HOME (mirror-sync, can prune)
databricks bundle run provision_optimizer -t <target>   # runs deploy/00_deploy.py
```

Note: `bundle deploy` mirror-syncs and **can prune** workspace files not in the repo — prefer `import-dir --overwrite` (step 1) once the optimizer has produced per-job runtime artifacts.

**Sync scripts** — thin wrappers over the four `import-dir` calls in step 1. Copy `deploy/env.example.sh` to `deploy/env.sh` (gitignored), fill in `PO_WORKSPACE_HOME` + `DATABRICKS_CONFIG_PROFILE`, then:

```bash
python deploy/sync_assets.py     # any OS (needs Python + the CLI)
bash   deploy/sync_assets.sh     # macOS / Linux only
```

## A new environment

Edit `config/optimizer.yaml` for the new catalog/schema/cluster and re-run step 2 (after the step-1 bootstrap against that workspace's profile). If you use DABs instead, copy the `latam` target block in `databricks.yml` to a new name and set its variables + `profile`. See [configuration.md](configuration.md) for what each variable means and how settings resolve at runtime.

## Repo layout

```
genie-code-pipeline-optimizer/
├── .assistant/skills/          Genie Code auto-loaded skills (Agent Skills spec)
│   ├── optimize-pipeline/          entry point: @optimize-pipeline (orchestrator, self-contained)
│   ├── detect-tables/              Genie reads the notebook → source/target tables (no parser)
│   ├── perf-profile/               root-cause diagnosis of the chosen notebook
│   ├── optimization-catalog/       reference recipes (a starting set, not a cage)
│   ├── optimize-notebook/          generate v2 <ntb>_genie_opt_<ts>
│   ├── sandbox-setup/              clone targets + pin inputs + remap writes
│   ├── equivalence-check/          step gate — counts → fingerprint → EXCEPT ALL, per notebook
│   ├── flow-validate/              flow gate — whole optimized job vs the original run's output
│   ├── perf-benchmark/             performance reading (median of N, advisory)
│   └── security-review/            late security gate before promotion
├── lib/                        importable harness — the skills import it, never reimplement:
│   ├── settings.py                 runtime config resolution + folder/identifier helpers
│   ├── config.py                   bootstrap_from_job, select_notebooks, sync_config
│   ├── perf.py                     rank_notebooks (hotspot ranking) + perf gate
│   ├── compute.py                  resolve_compute + run_notebook + benchmark_on_cluster
│   ├── sandbox.py                  clone_targets (full/sampled) + remap_writes
│   ├── equivalence.py              assert_equivalent + assert_no_protocol_change
│   ├── flow.py                     whole-job equivalence — replay / two-schema + compare + cleanup
│   ├── audit.py                    audit_log + assert_audited (no-audit-no-promote)
│   └── promote.py                  promote_notebook via the Jobs JSON (new job / in place)
├── sql/                        tables.sql · config_function.sql · governance_views.sql
├── config/                     optimizer.yaml (deploy config) · example_job.yaml (per-job opt_config reference)
├── deploy/                     provision.py (deploy_from_yaml) · 00_deploy.py · sync_assets.py · sync_assets.sh · env.example.sh
├── resources/                  provision_optimizer.job.yml (optional DABs job)
├── databricks.yml              optional DABs bundle + per-target overrides
└── docs/                       architecture · skills · configuration · deploy
```
