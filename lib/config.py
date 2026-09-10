"""opt_config: bootstrap from the job JSON, persist to UC, read at runtime.

Deterministic — reads the job's structured settings via the SDK (NOT a code parser).
Per-notebook source/target tables + operation are filled later by the detect-tables skill.
"""
from databricks.sdk import WorkspaceClient

from . import settings


def _config_table() -> str:
    """opt_config FQN, resolved at call time so settings.configure() takes effect."""
    return settings.optimizer_fqn("opt_config")

DEFAULTS = {"epsilon": 1e-6, "min_gain": 0.20, "benchmark_runs": 3, "validation_tier": "sampled",
            "compute_cluster_id": None}


def _resolve_job(w: WorkspaceClient, job_name: str):
    matches = [j for j in w.jobs.list(name=job_name)]
    if not matches:
        raise ValueError(f"No job named {job_name!r}")
    if len(matches) > 1:
        raise ValueError(f"{len(matches)} jobs named {job_name!r}; pass job_id to disambiguate")
    return w.jobs.get(matches[0].job_id)


def _dag_order(tasks) -> dict:
    """Topologically order tasks by depends_on -> {task_key: order}."""
    remaining = {t.task_key: [d.task_key for d in (t.depends_on or [])] for t in tasks}
    order, n = {}, 0
    while remaining:
        ready = [k for k, deps in remaining.items() if all(d not in remaining for d in deps)]
        if not ready:  # cycle / external dep -> fall back to declaration order
            ready = list(remaining)
        for k in sorted(ready):
            order[k], n = n, n + 1
            remaining.pop(k)
    return order


def _compute_of(settings) -> dict:
    """First cluster/warehouse the job uses (for fair before/after benchmarking)."""
    for t in settings.tasks or []:
        if t.existing_cluster_id:
            return {"cluster_id": t.existing_cluster_id}
        if getattr(t, "sql_task", None) and t.sql_task.warehouse_id:
            return {"warehouse_id": t.sql_task.warehouse_id}
    for jc in settings.job_clusters or []:
        return {"job_cluster_key": jc.job_cluster_key}
    return {}


def _prior_status(spark, job_name: str) -> dict:
    """{notebook_path: status} from any persisted opt_config for this job (empty if none)."""
    try:
        rows = spark.sql(f"SELECT notebook_path, status FROM {_config_table()} "
                         f"WHERE job_name = '{job_name}'").collect()
        return {r["notebook_path"]: r["status"] for r in rows}
    except Exception:  # table not provisioned yet / no rows -> fresh start
        return {}


def bootstrap_from_job(job_name: str, *, spark=None, sandbox_schema: str | None = None,
                       optimized_folder: str | None = None) -> dict:
    """job_name -> Jobs API -> draft opt_config (notebooks in DAG order, tables left for detect-tables).

    Pass `spark` to hydrate resolved history: notebooks already `promoted`/`blocked` in a prior run
    keep that status (the rest reset to `pending`), so selection stays resumable across sessions.
    """
    sandbox_schema = sandbox_schema or settings.SANDBOX_SCHEMA
    w = WorkspaceClient()
    job = _resolve_job(w, job_name)
    s = job.settings
    order = _dag_order(s.tasks or [])
    prior = _prior_status(spark, s.name) if spark is not None else {}

    notebooks = []
    for t in s.tasks or []:
        if not t.notebook_task:  # only notebook tasks are optimized here
            continue
        # Carry forward a terminal status from a prior run; otherwise start pending.
        prev = prior.get(t.notebook_task.notebook_path)
        notebooks.append({
            "notebook_path": t.notebook_task.notebook_path,
            "task_key": t.task_key,     # attributes runtime for hotspot ranking
            "dag_order": order.get(t.task_key, 0),
            "operation": None,          # detect-tables fills
            "source_tables": [],        # detect-tables fills (to pin)
            "target_tables": [],        # detect-tables fills (to clone)
            "equivalence_keys": [],
            "nondeterministic": None,   # detect-tables sets
            "status": prev if prev in ("promoted", "blocked") else "pending",
        })
    notebooks.sort(key=lambda x: x["dag_order"])

    defaults = dict(DEFAULTS)
    # Governed dedicated compute for heavy sandbox runs: default to the workspace setting; the job's
    # own compute and any explicit override are resolved later by compute.resolve_compute(cfg).
    defaults["compute_cluster_id"] = defaults["compute_cluster_id"] or settings.COMPUTE_CLUSTER_ID or None

    return {
        "job_name": s.name,
        "job_id": str(job.job_id),
        "sandbox_schema": sandbox_schema,
        "optimized_folder": optimized_folder or settings.optimized_folder(s.name),
        "compute": _compute_of(s),
        "defaults": defaults,
        "notebooks": notebooks,
    }


def sync_config(spark, cfg: dict) -> None:
    """Persist one row per notebook to opt_config (replace this job's rows). Idempotent.

    Explicit schema: Spark Connect can't infer types from rows with empty lists / None
    (e.g. an un-detected notebook has source_tables=[], operation=None).
    """
    from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                                    StringType, StructField, StructType)
    schema = StructType([
        StructField("job_name", StringType()), StructField("job_id", StringType()),
        StructField("notebook_path", StringType()), StructField("task_key", StringType()),
        StructField("dag_order", IntegerType()), StructField("operation", StringType()),
        StructField("source_tables", ArrayType(StringType())),
        StructField("target_tables", ArrayType(StringType())),
        StructField("equivalence_keys", ArrayType(StringType())),
        StructField("sandbox_schema", StringType()), StructField("optimized_folder", StringType()),
        StructField("epsilon", DoubleType()), StructField("min_gain", DoubleType()),
        StructField("benchmark_runs", IntegerType()), StructField("compute_cluster_id", StringType()),
        StructField("nondeterministic", BooleanType()), StructField("status", StringType()),
    ])
    d, nb = cfg["defaults"], cfg["notebooks"]
    rows = [{
        "job_name": cfg["job_name"], "job_id": cfg["job_id"], "notebook_path": n["notebook_path"],
        "task_key": n.get("task_key"),
        "dag_order": int(n["dag_order"]), "operation": n["operation"],
        "source_tables": n["source_tables"] or [], "target_tables": n["target_tables"] or [],
        "equivalence_keys": n["equivalence_keys"] or [], "sandbox_schema": cfg["sandbox_schema"],
        "optimized_folder": cfg["optimized_folder"], "epsilon": float(d["epsilon"]),
        "min_gain": float(d["min_gain"]), "benchmark_runs": int(d["benchmark_runs"]),
        "compute_cluster_id": d.get("compute_cluster_id"),
        "nondeterministic": n["nondeterministic"], "status": n["status"],
    } for n in nb]
    table = _config_table()
    spark.sql(f"DELETE FROM {table} WHERE job_name = '{cfg['job_name']}'")
    spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(table)


def load_config(spark, job_name: str) -> list[dict]:
    """Read the persisted opt_config rows for a job (DAG order)."""
    df = spark.sql(f"SELECT * FROM {_config_table()} WHERE job_name = '{job_name}' ORDER BY dag_order")
    return [r.asDict(recursive=True) for r in df.collect()]


def _matches(n: dict, pick) -> bool:
    """A pick is a dag_order int, or a substring of the notebook path (name works)."""
    if isinstance(pick, bool):
        return False
    if isinstance(pick, int):
        return n["dag_order"] == pick
    return str(pick) in n["notebook_path"]


def select_notebooks(cfg: dict, picks=None) -> dict:
    """Choose which notebooks to optimize THIS run — the gradual, one-step-at-a-time control.

    `picks` = a dag_order int, a path/name substring, or a list of them; `None`/`"all"` selects
    every notebook. Selected notebooks are left `pending` (they will run); the rest become
    `skipped`. Terminal states from earlier runs (`promoted`, `blocked`) are never disturbed, so
    the flow is resumable: run notebook 5 today, notebook 0 next session. Mutates and returns cfg.
    """
    nb = cfg["notebooks"]
    if picks in (None, "all", ["all"]):
        chosen = set(range(len(nb)))
    else:
        picks = picks if isinstance(picks, (list, tuple)) else [picks]
        chosen = {i for i, n in enumerate(nb) if any(_matches(n, p) for p in picks)}
    if not chosen:
        raise ValueError(f"No notebook matched picks={picks!r}; available: "
                         f"{[(n['dag_order'], n['notebook_path']) for n in nb]}")
    for i, n in enumerate(nb):
        if n["status"] in ("promoted", "blocked"):  # keep resolved history
            continue
        n["status"] = "pending" if i in chosen else "skipped"
    return cfg


def pending_notebooks(cfg: dict) -> list[dict]:
    """Selected notebooks still to process, in DAG order (status == 'pending')."""
    return sorted((n for n in cfg["notebooks"] if n["status"] == "pending"),
                  key=lambda x: x["dag_order"])


_TERMINAL = ("promoted", "blocked", "skipped", "pending")


def set_notebook_status(spark, job_name: str, notebook_path: str, status: str) -> None:
    """Advance ONE notebook's lifecycle status in opt_config so the table stays a truthful progress
    board (not just the audit trail). Written by the flow: `promote` -> 'promoted', any gate STOP ->
    'blocked'. Also mirrors it into the in-memory cfg if the caller keeps one. RAISES if no row.
    """
    if status not in _TERMINAL:
        raise ValueError(f"status must be one of {_TERMINAL}, got {status!r}")
    table = _config_table()
    n = spark.sql(f"SELECT count(*) c FROM {table} WHERE job_name = '{job_name}' "
                  f"AND notebook_path = '{notebook_path}'").collect()[0]["c"]
    if not n:
        raise ValueError(f"No opt_config row for {job_name}/{notebook_path} to set status={status!r} "
                         "— sync_config was never called.")
    spark.sql(f"UPDATE {table} SET status = '{status}' WHERE job_name = '{job_name}' "
              f"AND notebook_path = '{notebook_path}'")
