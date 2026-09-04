"""opt_config: bootstrap from the job JSON, persist to UC, read at runtime."""


def bootstrap_from_job(job_name: str) -> dict:
    """job_name -> Jobs API get job -> draft opt_config (tasks, notebooks, compute).

    Auto-fills notebooks in DAG order. Per-notebook source/target tables are filled by
    the table-detection step (code parse / lineage / dry-run) before optimization.
    """
    # TODO: databricks.sdk WorkspaceClient().jobs.get(...) -> parse settings.tasks.
    raise NotImplementedError


def load_config(spark, job_name: str) -> dict:
    """Read the persisted opt_config for a job from the factory schema."""
    raise NotImplementedError


def sync_config(spark, cfg: dict) -> None:
    """MERGE a config dict into the opt_config table (explicit column list)."""
    raise NotImplementedError
