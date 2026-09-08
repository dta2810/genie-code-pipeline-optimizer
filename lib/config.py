"""opt_config: bootstrap from the job JSON, persist to UC, read at runtime.

Deterministic — reads the job's structured settings via the SDK (NOT a code parser).
Per-notebook source/target tables + operation are filled later by the detect-tables skill.
"""
from databricks.sdk import WorkspaceClient

from . import settings


def _config_table() -> str:
    """opt_config FQN, resolved at call time so settings.configure() takes effect."""
    return settings.factory_fqn("opt_config")

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


def bootstrap_from_job(job_name: str, *, sandbox_schema: str | None = None,
                       optimized_folder: str | None = None) -> dict:
    """job_name -> Jobs API -> draft opt_config (notebooks in DAG order, tables left for detect-tables)."""
    sandbox_schema = sandbox_schema or settings.SANDBOX_SCHEMA
    w = WorkspaceClient()
    job = _resolve_job(w, job_name)
    s = job.settings
    order = _dag_order(s.tasks or [])

    notebooks = []
    for t in s.tasks or []:
        if not t.notebook_task:  # only notebook tasks are optimized here
            continue
        notebooks.append({
            "notebook_path": t.notebook_task.notebook_path,
            "task_key": t.task_key,     # attributes runtime for hotspot ranking
            "dag_order": order.get(t.task_key, 0),
            "operation": None,          # detect-tables fills
            "source_tables": [],        # detect-tables fills (to pin)
            "target_tables": [],        # detect-tables fills (to clone)
            "equivalence_keys": [],
            "nondeterministic": None,   # detect-tables sets
            "status": "pending",
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
        "optimized_folder": optimized_folder or f"{settings.optimized_folder()}/{s.name}",
        "compute": _compute_of(s),
        "defaults": defaults,
        "notebooks": notebooks,
    }


def sync_config(spark, cfg: dict) -> None:
    """Persist one row per notebook to opt_config (replace this job's rows). Idempotent."""
    d, nb = cfg["defaults"], cfg["notebooks"]
    rows = [{
        "job_name": cfg["job_name"], "job_id": cfg["job_id"], "notebook_path": n["notebook_path"],
        "task_key": n.get("task_key"),
        "dag_order": n["dag_order"], "operation": n["operation"],
        "source_tables": n["source_tables"], "target_tables": n["target_tables"],
        "equivalence_keys": n["equivalence_keys"], "sandbox_schema": cfg["sandbox_schema"],
        "optimized_folder": cfg["optimized_folder"], "epsilon": d["epsilon"],
        "min_gain": d["min_gain"], "benchmark_runs": d["benchmark_runs"],
        "compute_cluster_id": d.get("compute_cluster_id"),
        "nondeterministic": n["nondeterministic"], "status": n["status"],
    } for n in nb]
    table = _config_table()
    spark.sql(f"DELETE FROM {table} WHERE job_name = '{cfg['job_name']}'")
    spark.createDataFrame(rows).write.mode("append").saveAsTable(table)


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
