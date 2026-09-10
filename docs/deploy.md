# Deploy

## Prerequisites

- The **Databricks CLI** installed and on your PATH (`databricks --version`).

- An **authenticated CLI profile** for your workspace, created with `databricks auth login` (it lands in `~/.databrickscfg`). Everything below refers to that profile as `<profile>`.

- Permission in the workspace to create a schema in your optimizer catalog, write under your `WORKSPACE_HOME`, and submit job runs on the dedicated cluster.

## Target vs. profile (they are not the same thing)

A **bundle target** is an environment defined in `databricks.yml` under `targets:`. It carries the per-environment variable overrides and points at a CLI profile. `-t <target>` selects it.

A **CLI profile** is the workspace host + credentials in `~/.databrickscfg`.

The repo ships one example target named `latam` that uses the profile `LATAM`. Rename it (or add your own target block) for your environment — see "A new environment" below. When a command shows `-t <target>`, substitute your target name.

## Deploy with DABs (recommended, cross-platform)

`bundle` is pure CLI, so this path works the same on Windows, macOS, and Linux. Locations are bundle variables, overridden per target in `databricks.yml` — one target per environment, no code edits.

```bash
databricks bundle validate --strict -t <target>
databricks bundle deploy -t <target>                    # syncs skills + lib + sql + deploy to WORKSPACE_HOME
databricks bundle run provision_optimizer -t <target>   # creates schema + tables + views + get_opt_config
```

`deploy` lands the code at `WORKSPACE_HOME` (no `/files` subdir) so Genie Code auto-loads `.assistant/skills` and the `lib/` imports resolve.

`provision_optimizer` runs `deploy/00_deploy.py`, which is idempotent (`CREATE … IF NOT EXISTS` / `OR REPLACE`, and it ALTERs new columns onto an existing `opt_config`). Run it only on first deploy or when `sql/` or the `opt_config` schema changed.

The bundle reads its configuration from `databricks.yml` (the target's variables), **not** from `deploy/env.sh` — that file is for the manual path below.

## Incremental code-only updates

To push edited skills or `lib` to an existing deployment without re-provisioning, sync the asset directories with `import-dir --overwrite` — it adds and overwrites only the dirs it copies and never prunes, so it is safe over the per-job runtime artifacts (`jobs/<job>/optimized|validation|driver`). Prefer this over `bundle deploy` once the optimizer has produced those artifacts, because `bundle deploy` mirror-syncs and can prune workspace files that aren't in the repo.

First set where things live (copy the template, fill it in, and source it):

```bash
cp deploy/env.example.sh deploy/env.sh   # then edit deploy/env.sh — at least PO_WORKSPACE_HOME + DATABRICKS_CONFIG_PROFILE
source deploy/env.sh
```

Then run the sync — pick the one for your OS:

```bash
python deploy/sync_assets.py     # any OS (Windows / macOS / Linux) — needs Python + the CLI
bash   deploy/sync_assets.sh     # macOS / Linux only
```

Both do the same four `databricks workspace import-dir` calls. If you'd rather not use a script at all, run them directly (cross-platform) — this is the Windows-without-bash path:

```
databricks workspace import-dir .assistant  <WORKSPACE_HOME>/.assistant --overwrite --profile <profile>
databricks workspace import-dir lib          <WORKSPACE_HOME>/lib        --overwrite --profile <profile>
databricks workspace import-dir deploy       <WORKSPACE_HOME>/deploy     --overwrite --profile <profile>
databricks workspace import-dir sql          <WORKSPACE_HOME>/sql        --overwrite --profile <profile>
```

## A new environment

Copy the `latam` target block in `databricks.yml` to a new name and set its `optimizer_catalog`, `optimizer_schema`, `sandbox_schema`, `compute_cluster_id`, and `profile`. Then `databricks bundle deploy -t <your-target>`. See [configuration.md](configuration.md) for what each variable means and how settings are resolved at runtime.

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
├── deploy/                     provision.py · 00_deploy.py · sync_assets.py · sync_assets.sh · env.example.sh
├── resources/                  provision_optimizer.job.yml (DABs job)
├── databricks.yml              bundle vars + per-target overrides
└── docs/                       architecture · skills · configuration · deploy
```
