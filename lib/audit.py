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


def _truthy(v):
    return v if isinstance(v, bool) else str(v).strip().lower() in ("true", "passed", "pass", "1", "yes")


def _norm_change_type(ct):
    """ARRAY<STRING> — accept a list OR a comma-joined string. Never `list("abc")` char-explode
    (the #1 audit bug: a string arg iterated char-by-char). Returns a clean list or None.
    """
    if not ct:
        return None
    parts = ([t.strip() for t in ct.split(",")] if isinstance(ct, str)
             else [str(t).strip() for t in ct])
    return [p for p in parts if p] or None


def _canonical_equivalence(m):
    """Guarantee the key the scorecard reads (`result`); derive it from `passed` if only that exists.
    Lets an inline caller that used a different key still surface in v_optimization_scorecard.
    """
    if not m:
        return m
    m = dict(m)
    if "result" not in m and "passed" in m:
        m["result"] = "passed" if _truthy(m["passed"]) else "failed"
    return m


def _canonical_perf(m):
    """Guarantee the keys the scorecard reads (`gain`, `passed`); accept common aliases."""
    if not m:
        return m
    m = dict(m)
    if "gain" not in m and "gain_pct" in m:
        m["gain"] = m["gain_pct"]
    if "passed" not in m and "result" in m:
        m["passed"] = _truthy(m["result"])
    return m


def equivalence_map(*, passed, risk_tier=None, **detail):
    """Build a canonical equivalence audit map (always has `result`). Prefer this over a raw dict."""
    m = {"result": "passed" if _truthy(passed) else "failed"}
    if risk_tier is not None:
        m["risk_tier"] = risk_tier
    m.update(detail)
    return m


def perf_map(*, gain, passed, before_s=None, after_s=None, **detail):
    """Build a canonical perf audit map (always has `gain` + `passed`). Prefer this over a raw dict."""
    m = {"gain": gain, "passed": _truthy(passed)}
    if before_s is not None:
        m["before_s"] = before_s
    if after_s is not None:
        m["after_s"] = after_s
    m.update(detail)
    return m


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
        "change_type": _norm_change_type(change_type),
        "equivalence": _str_map(_canonical_equivalence(equivalence)),
        "perf": _str_map(_canonical_perf(perf)),
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
    cfg_rows = spark.sql(f"SELECT status FROM {cfg_tbl} "
                         f"WHERE job_name = '{job}' AND notebook_path = '{notebook}'").collect()
    if not cfg_rows:
        raise ValueError(
            f"Promotion BLOCKED: opt_config has no row for {job}/{notebook} — sync_config was "
            "never called (config kept only in memory). Persist it before promoting.")
    status = cfg_rows[0]["status"]
    # A 'skipped' notebook was never selected this run — promoting it means selection was bypassed.
    if status == "skipped":
        raise ValueError(
            f"Promotion BLOCKED: {job}/{notebook} is 'skipped' in opt_config — it was not selected "
            "for this run. Call select_notebooks + sync_config to mark it 'pending' first.")

    trail = audit_trail(spark, job=job, notebook=notebook)
    ok = {r["step"] for r in trail if r["status"] == "succeeded"}
    missing = [s for s in required if s not in ok]
    if missing:
        raise ValueError(
            f"Promotion BLOCKED: no `succeeded` audit trail for {missing} on {job}/{notebook}. "
            "The harness (audit_step/audit_log) was not used — re-run the flow through the "
            "wrappers so every step is recorded, then promote.")
    return {"audited": True, "steps_recorded": sorted(ok), "events": len(trail),
            "config_status": status}


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
