"""Append-only audit logging for the optimization factory. Import from every skill."""
from contextlib import contextmanager

from .settings import factory_fqn

AUDIT_TABLE = factory_fqn("optimization_audit")


def audit_log(spark, *, job, notebook, step, status, change_type=None,
              equivalence=None, perf=None, insight=None):
    """Append one audit event. status in {started, succeeded, failed}."""
    # TODO: INSERT one row into AUDIT_TABLE (see sql/tables.sql for the schema).
    raise NotImplementedError


@contextmanager
def audit_step(spark, *, job, notebook, step, **kw):
    """Wrap a step: log started, then succeeded — or failed + re-raise."""
    audit_log(spark, job=job, notebook=notebook, step=step, status="started", **kw)
    try:
        yield
    except Exception as e:  # log the failure, then propagate
        audit_log(spark, job=job, notebook=notebook, step=step, status="failed",
                  insight=str(e)[:500])
        raise
    audit_log(spark, job=job, notebook=notebook, step=step, status="succeeded", **kw)
