"""equivalence-check: prove candidate == baseline, audited. Wraps lib.equivalence."""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 4)))

from lib.audit import audit_log  # noqa: E402
from lib.equivalence import assert_equivalent  # noqa: E402


def check(spark, *, job, notebook, baseline, candidate, epsilon=1e-6, partition_cols=None):
    """Run the ladder; audit pass/fail with insight. RAISES on divergence (hard gate)."""
    try:
        res = assert_equivalent(spark, baseline, candidate,
                                epsilon=epsilon, partition_cols=partition_cols)
    except ValueError as e:
        audit_log(spark, job=job, notebook=notebook, step="equivalence", status="failed",
                  equivalence={"result": "failed"}, insight=str(e)[:500])
        raise
    audit_log(spark, job=job, notebook=notebook, step="equivalence", status="succeeded",
              equivalence={"result": "passed", "method": "counts+fingerprint+except_all"},
              insight="baseline == candidate within epsilon")
    return res
