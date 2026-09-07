"""equivalence-check: prove candidate == baseline, audited. Wraps lib.equivalence."""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 4)))

from lib.audit import audit_log  # noqa: E402
from lib.equivalence import assert_equivalent  # noqa: E402


def check(spark, *, job, notebook, baseline, candidate, risk_tier="semantics_preserving",
          epsilon=None, partition_cols=None, baseline_is_full_recompute=False):
    """Run the ladder; audit pass/fail with insight. RAISES on divergence (hard gate).

    `risk_tier` comes from the technique applied (optimization-catalog) and sets the epsilon
    policy: zero_risk -> exact, semantics_preserving / refresh_change -> tolerance.
    """
    try:
        res = assert_equivalent(spark, baseline, candidate, risk_tier=risk_tier,
                                epsilon=epsilon, partition_cols=partition_cols,
                                baseline_is_full_recompute=baseline_is_full_recompute)
    except ValueError as e:
        audit_log(spark, job=job, notebook=notebook, step="equivalence", status="failed",
                  equivalence={"result": "failed", "risk_tier": risk_tier}, insight=str(e)[:500])
        raise
    audit_log(spark, job=job, notebook=notebook, step="equivalence", status="succeeded",
              equivalence={"result": "passed", "risk_tier": risk_tier,
                           "epsilon": res["epsilon"],
                           "method": "counts+fingerprint+except_all(full)"},
              insight=f"baseline == candidate ({risk_tier}, epsilon={res['epsilon']})")
    return res
