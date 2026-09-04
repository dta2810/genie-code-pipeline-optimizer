"""Append-only audit logging for the optimization factory. Import from every skill."""
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from .settings import factory_fqn

AUDIT_TABLE = factory_fqn("optimization_audit")


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
    spark.createDataFrame([row], schema).write.mode("append").saveAsTable(AUDIT_TABLE)


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
