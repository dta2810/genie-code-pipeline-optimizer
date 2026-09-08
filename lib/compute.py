"""Governed compute selection: run the HEAVY sandbox work on a dedicated cluster, not the
serverless Genie Code session.

Why: serverless is fine for control work (config, detect-tables, audit, equivalence on a bounded
sample) but it cold-starts and loses cell state on autoscale, so a 180M-row rewrite times out and
its wall-clock is contaminated (setup, not I/O). The fix — same pattern the Model Factory uses for
its EDA/training — is to submit the actual v1/v2 notebook runs to a warm, dedicated cluster via
`jobs runs submit` with `existing_cluster_id`, and read the run's `execution_duration` for a clean,
comparable timing. Which cluster is GOVERNED in opt_config (`compute_cluster_id`).

Routing (see optimize-pipeline): control work stays on the session; the notebook runs + wall-clock
benchmark go to the resolved dedicated cluster.
"""
import time

from databricks.sdk import WorkspaceClient

from . import settings


def resolve_compute(cfg: dict, override: str | None = None) -> dict:
    """Pick the cluster that runs the heavy sandbox/benchmark work.

    Precedence: explicit `override` > `opt_config.compute_cluster_id` (governed) > the job's own
    compute (`cfg["compute"]`, captured at bootstrap) > the default cluster from settings > None
    (fall back to the serverless session, with a warning from the caller).
    """
    if override:
        return {"cluster_id": override, "source": "override"}
    cid = (cfg.get("defaults", {}) or {}).get("compute_cluster_id")
    if cid:
        return {"cluster_id": cid, "source": "opt_config"}
    job_compute = cfg.get("compute", {}) or {}
    if job_compute.get("cluster_id"):
        return {"cluster_id": job_compute["cluster_id"], "source": "job_compute"}
    if settings.COMPUTE_CLUSTER_ID:
        return {"cluster_id": settings.COMPUTE_CLUSTER_ID, "source": "settings_default"}
    return {"cluster_id": None, "source": "serverless_session"}


def run_notebook(notebook_path: str, cluster_id: str, *, base_parameters: dict | None = None,
                 timeout_s: int = 3600, poll_s: int = 10, run_name: str | None = None) -> dict:
    """Submit a ONE-TIME job run of `notebook_path` on `cluster_id`, wait, and return the result
    with a clean execution time.

    Uses `POST /jobs/runs/submit` (raw REST, version-safe — same style as promote.py). Timing =
    the run's `execution_duration` (command time on the warm cluster), which excludes cluster
    setup, so it is a fair before/after number even on a shared cluster.
    """
    w = WorkspaceClient()
    body = {
        "run_name": run_name or f"opt-run {notebook_path.split('/')[-1]}",
        "tasks": [{
            "task_key": "run",
            "existing_cluster_id": cluster_id,
            "notebook_task": {"notebook_path": notebook_path,
                              "base_parameters": base_parameters or {}},
        }],
    }
    run_id = w.api_client.do("POST", "/api/2.2/jobs/runs/submit", body=body)["run_id"]

    deadline = time.time() + timeout_s
    while True:
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        state = run.get("state", {})
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        if time.time() > deadline:
            raise TimeoutError(f"run {run_id} not done after {timeout_s}s "
                               f"(state {state.get('life_cycle_state')})")
        time.sleep(poll_s)

    result_state = state.get("result_state")
    task = (run.get("tasks") or [{}])[0]
    exec_ms = task.get("execution_duration") or run.get("execution_duration") or 0
    out = {"run_id": run_id, "result_state": result_state,
           "execution_s": exec_ms / 1000.0, "run_page_url": run.get("run_page_url")}
    if result_state != "SUCCESS":
        raise RuntimeError(f"run {run_id} finished {result_state}: {state.get('state_message')} "
                           f"({out['run_page_url']})")
    return out


def benchmark_on_cluster(baseline_path: str, candidate_path: str, cluster_id: str, *,
                         base_parameters: dict | None = None, runs: int = 3, warmup: bool = True,
                         timeout_s: int = 3600) -> dict:
    """Clean before/after timing on a dedicated cluster: submit baseline and candidate `runs` times
    each (one discarded warmup), median of `execution_duration`. Baseline first, same cluster.

    This is the honest wall-clock — a warm dedicated cluster, `execution_duration` only. On
    serverless there is no reliable wall-clock; use the structural I/O signal instead (perf-benchmark).
    """
    import statistics

    def _series(path):
        if warmup:
            run_notebook(path, cluster_id, base_parameters=base_parameters, timeout_s=timeout_s)
        xs = [run_notebook(path, cluster_id, base_parameters=base_parameters,
                           timeout_s=timeout_s)["execution_s"] for _ in range(runs)]
        return {"runs_s": xs, "median_s": statistics.median(xs), "min_s": min(xs), "max_s": max(xs)}

    return {"cluster_id": cluster_id, "baseline": _series(baseline_path),
            "candidate": _series(candidate_path)}
