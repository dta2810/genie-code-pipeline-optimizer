# Configuration

Two levels, both resolved at runtime — never hard-coded.

## 1 · Where the optimizer lives

Resolved by `lib/settings.py`, in this order:

```
explicit configure(...) arg  >  runtime config file  >  auto-discovery  >  env var  >  default
```


| Setting              | Env var                 | Default                                       |
| -------------------- | ----------------------- | --------------------------------------------- |
| `OPTIMIZER_CATALOG`  | `PO_OPTIMIZER_CATALOG`  | `main`                                        |
| `OPTIMIZER_SCHEMA`   | `PO_OPTIMIZER_SCHEMA`   | `genie_optimizer`                             |
| `SANDBOX_SCHEMA`     | `PO_SANDBOX_SCHEMA`     | `pipeline_opt_sandbox`                        |
| `WORKSPACE_HOME`     | `PO_WORKSPACE_HOME`     | `/Workspace/Users/<you>/genie_code_optimizer` |
| `COMPUTE_CLUSTER_ID` | `PO_COMPUTE_CLUSTER_ID` | *(empty → serverless)*                        |


Env vars don't reach the Databricks runtime, so `provision` writes a small runtime config file at `/Workspace/Users/{user}/.genie_optimizer.json` (outside the bundle sync root, so redeploys never delete it), and `settings.configure(spark=spark)` reads it.


If the file is absent, `configure()` auto-discovers an existing optimizer schema by scanning for its `opt_config` table and adopts it, rather than provisioning a duplicate.

**How to set these.** With a bundle deploy they come from the target's variables in `databricks.yml` (see [deploy.md](deploy.md)). For the manual sync/provision path, copy `deploy/env.example.sh` to `deploy/env.sh`, fill it in, and `source deploy/env.sh` before running — including `PO_COMPUTE_CLUSTER_ID` for the dedicated cluster. `provision` then writes the runtime config file so the deployed skills resolve the same values.

## 2 · Per-job and per-notebook

The `opt_config` table, seeded by `bootstrap_from_job`:


| Column                            | Default                  | Purpose                                                     |
| --------------------------------- | ------------------------ | ----------------------------------------------------------- |
| `epsilon`                         | `1e-6`                   | equivalence float tolerance                                 |
| `min_gain`                        | `0.20`                   | advisory performance threshold                              |
| `benchmark_runs`                  | `3`                      | runs for the median                                         |
| `validation_tier`                 | `sampled`                | sample big targets so equivalence stays serverless-friendly |
| `compute_cluster_id`              | *(inherits the setting)* | governed dedicated compute, per job                         |
| `source_tables` / `target_tables` | *(empty)*                | filled by `detect-tables` reading the notebook              |




## Workspace layout

Everything the optimizer creates lands under `WORKSPACE_HOME`, addressed by a `settings` helper — never a hard-coded path, never a stray agent folder.
One root, one convention:

```
WORKSPACE_HOME/  (= genie_code_optimizer/)
└── jobs/<job>/                          settings.job_home(job)
    ├── optimized/   <ntb>_genie_opt_<ts>    settings.optimized_folder(job)   ← v2 candidates
    ├── validation/  <ntb>_validate_<ts>     settings.validation_folder(job)  ← per-notebook run notebooks
    └── driver/      the orchestrator notebook + run traceability   settings.driver_folder(job)

UC sandbox schema:   cat.<SANDBOX_SCHEMA>.<schema>__<table>   settings.sandbox_fqn(...)   (dropped after; flow.cleanup_replay)

UC optimizer schema: opt_config · optimization_audit · governance views · get_opt_config
```

`optimized_folder` requires a `job_name` — there is no shared top-level folder, which used to strand notebooks outside the per-job layout.


`settings.assert_under_home(path)` raises on any path that escapes `WORKSPACE_HOME`.


The Genie driver notebook, which a session may open in a default agent folder, is relocated under `driver/` at bootstrap so nothing the framework makes lives outside the home.

The sandbox is schema-based, inside each target's own catalog: a clone of `cat.schema.table` lands at `cat.<SANDBOX_SCHEMA>.<schema>__<table>`.


No `CREATE CATALOG` privilege is needed, and tables from different schemas never collide.

## Compute — where the heavy work runs

Control work (config, detect-tables, audit, equivalence on a bounded sample) runs on the serverless Genie Code session.


The heavy work — running the v1/v2 notebooks and the wall-clock benchmark — is submitted to a dedicated cluster via `jobs runs submit` and `existing_cluster_id`, because serverless cold-starts and loses cell state on autoscale (large rewrites time out) and its wall-clock is contaminated by setup.



`lib.compute.resolve_compute(cfg)` picks the cluster in this order:

```
explicit override  >  opt_config.compute_cluster_id  >  the job's own compute  >  settings default  >  serverless
```

If it resolves to serverless, the run warns you and reports the structural I/O signal (rows and files rewritten) instead of an unreliable wall-clock.