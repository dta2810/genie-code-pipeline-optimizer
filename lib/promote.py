"""Promote a validated v2 by repointing a job task's notebook to it — via the Jobs JSON.

Scope (this iteration): create a NEW job (or update in place) whose task points at the v2 notebook.
No PR/DAB. Default is a NEW job so the original is untouched and rollback is just deleting the new
job. Manipulates the job settings dict directly (version-safe) via the raw REST API.
"""
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient


def _repoint(body: dict, task_key: str, v2_path: str) -> bool:
    """Point the given task's notebook at v2_path (handles for_each-nested notebook tasks)."""
    for t in body.get("tasks", []):
        if t.get("task_key") != task_key:
            continue
        if "notebook_task" in t:
            t["notebook_task"]["notebook_path"] = v2_path
            return True
        inner = t.get("for_each_task", {}).get("task", {})
        if "notebook_task" in inner:
            inner["notebook_task"]["notebook_path"] = v2_path
            return True
    return False


def promote_notebook(job_id, task_key: str, v2_path: str, *, new_job: bool = True,
                     spark=None, job_name: str | None = None,
                     source_notebook_path: str | None = None) -> dict:
    """Repoint `task_key`'s notebook to `v2_path`. new_job=True (default) clones the job settings
    into a NEW job (original untouched); new_job=False updates the existing job in place.

    Pass `spark` + `job_name` + `source_notebook_path` and the promotion also advances that
    notebook's `opt_config.status` to 'promoted' — so the progress board reflects reality instead of
    being left stale on 'pending' (the agent no longer has to remember to update it by hand).
    """
    w = WorkspaceClient()
    body = w.jobs.get(int(job_id)).settings.as_dict()
    if not _repoint(body, task_key, v2_path):
        raise ValueError(f"No notebook task {task_key!r} found in job {job_id}")

    if new_job:
        # Dated name so successive promotions don't collide + the original stays traceable.
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        base = body.get("name", "job")
        body["name"] = f"{base} (genie-opt {stamp})"
        body["description"] = (f"Optimized from job {job_id} by genie_code_optimizer "
                               f"({datetime.now(timezone.utc).isoformat(timespec='minutes')}). "
                               f"Task {task_key} -> {v2_path}. Original job untouched.")
        res = w.api_client.do("POST", "/api/2.2/jobs/create", body=body)
        result = {"mode": "new_job", "source_job_id": str(job_id),
                  "new_job_id": res.get("job_id"), "task_key": task_key, "notebook_path": v2_path}
    else:
        w.api_client.do("POST", "/api/2.2/jobs/reset",
                        body={"job_id": int(job_id), "new_settings": body})
        result = {"mode": "in_place", "job_id": str(job_id),
                  "task_key": task_key, "notebook_path": v2_path}

    # Advance the progress board once the job actually changed.
    if spark is not None and job_name and source_notebook_path:
        from .config import set_notebook_status
        set_notebook_status(spark, job_name, source_notebook_path, "promoted")
        result["status_updated"] = "promoted"
    return result
