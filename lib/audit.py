"""Append-only audit logging for the pipeline optimizer. Import from every skill."""
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from . import settings


def _str_map(d):
    """MAP<STRING,STRING> — coerce every value to str (audit is descriptive, not numeric)."""
    if not d:
        return None
    return {str(k): (None if v is None else str(v)) for k, v in d.items()}


def audit_log(spark, *, job, notebook, step, status, change_type=None,
              equivalence=None, perf=None, notebook_path=None, insight=None):
    """Append one audit event. status in {started, succeeded, failed}."""
    from pyspark.sql.types import (ArrayType, MapType, StringType, StructField,
                                   StructType, TimestampType)
    schema = StructType([
        StructField("audit_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("user", StringType()),
        StructField("job", StringType()),
        StructField("notebook", StringType()),
        StructField("step", StringType()),
        StructField("status", StringType()),
        StructField("change_type", ArrayType(StringType())),
        StructField("equivalence", MapType(StringType(), StringType())),
        StructField("perf", MapType(StringType(), StringType())),
        StructField("notebook_path", StringType()),
        StructField("insight", StringType()),
    ])
    try:
        user = spark.sql("SELECT current_user()").collect()[0][0]
    except Exception:
        user = None
    row = {
        "audit_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc),
        "user": user, "job": job, "notebook": notebook, "step": step, "status": status,
        "change_type": list(change_type) if change_type else None,
        "equivalence": _str_map(equivalence), "perf": _str_map(perf),
        "notebook_path": notebook_path, "insight": insight,
    }
    spark.createDataFrame([row], schema).write.mode("append").saveAsTable(
        settings.optimizer_fqn("optimization_audit"))


def audit_trail(spark, *, job, notebook=None) -> list[dict]:
    """Read the audit events for a job (optionally one notebook), oldest first."""
    tbl = settings.optimizer_fqn("optimization_audit")
    where = f"job = '{job}'" + (f" AND notebook = '{notebook}'" if notebook else "")
    rows = spark.sql(f"SELECT step, status, event_ts, insight FROM {tbl} "
                     f"WHERE {where} ORDER BY event_ts").collect()
    return [r.asDict() for r in rows]


# Steps that must be recorded before a candidate may be promoted.
REQUIRED_STEPS = ("detect_tables", "perf_profile", "generate_v2", "sandbox_setup",
                  "equivalence", "perf_benchmark", "security_review")


def assert_audited(spark, *, job, notebook, required=REQUIRED_STEPS) -> dict:
    """No-audit-no-promote gate: every required step must have a `succeeded` audit row for this
    job+notebook, or promotion is REFUSED. Makes an un-audited run (harness bypassed / reimplemented
    inline) impossible to promote past. Call right before GATE 2.
    """
    # opt_config must be persisted too (sync_config was actually called, not just kept in memory).
    cfg_tbl = settings.optimizer_fqn("opt_config")
    n_cfg = spark.sql(f"SELECT count(*) c FROM {cfg_tbl} "
                      f"WHERE job_name = '{job}' AND notebook_path = '{notebook}'").collect()[0]["c"]
    if not n_cfg:
        raise ValueError(
            f"Promotion BLOCKED: opt_config has no row for {job}/{notebook} — sync_config was "
            "never called (config kept only in memory). Persist it before promoting.")

    trail = audit_trail(spark, job=job, notebook=notebook)
    ok = {r["step"] for r in trail if r["status"] == "succeeded"}
    missing = [s for s in required if s not in ok]
    if missing:
        raise ValueError(
            f"Promotion BLOCKED: no `succeeded` audit trail for {missing} on {job}/{notebook}. "
            "The harness (audit_step/audit_log) was not used — re-run the flow through the "
            "wrappers so every step is recorded, then promote.")
    return {"audited": True, "steps_recorded": sorted(ok), "events": len(trail), "config_rows": n_cfg}


@contextmanager
def audit_step(spark, *, job, notebook, step, **terminal):
    """Wrap a step: log started (minimal), then succeeded (+terminal fields) — or failed + re-raise."""
    audit_log(spark, job=job, notebook=notebook, step=step, status="started")
    try:
        yield
    except Exception as e:  # log the failure with its message, then propagate
        audit_log(spark, job=job, notebook=notebook, step=step, status="failed",
                  insight=str(e)[:500])
        raise
    audit_log(spark, job=job, notebook=notebook, step=step, status="succeeded", **terminal)
