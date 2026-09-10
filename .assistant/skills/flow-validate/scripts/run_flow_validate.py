"""flow-validate: prove whole optimized job == whole original job, audited. Wraps lib.flow."""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 4)))

from lib import flow  # noqa: E402


def run(spark, *, job_name, orig_job_id, opt_job_id, catalog, src_schema, tables, targets,
        cluster_id, sample_percent=None, sample_only=None, seed=42, partition_cols=None,
        risk_tier="semantics_preserving", run_timeout_s=3600):
    """Flow-level gate. `tables` = every source + target; `targets` = final tables to compare.

    `sample_only` (+ `sample_percent`) samples a huge driver so the original job's baseline stays
    cheap; the same seed makes _orig/_opt identical, so the comparison is still fair. Returns a
    verdict dict; audits `flow_equivalence`. RAISES only on operational failure, not a clean FAIL.
    """
    verdict = flow.flow_validate(
        spark, job_name=job_name, orig_job_id=orig_job_id, opt_job_id=opt_job_id,
        catalog=catalog, src_schema=src_schema, tables=tables, targets=targets,
        cluster_id=cluster_id, sample_percent=sample_percent, sample_only=sample_only,
        seed=seed, partition_cols=partition_cols, risk_tier=risk_tier, run_timeout_s=run_timeout_s)
    _print(verdict)
    return verdict


def run_replay(spark, *, job_name, opt_job_id, catalog, src_schema, tables, targets, cluster_id,
               anchor_before, anchor_after, table_pins=None, target_pins=None,
               partition_cols=None, risk_tier="semantics_preserving", run_timeout_s=3600):
    """Replay against a real past run (ground truth) — the faithful test: pin inputs to when the
    original pipeline ran (`anchor_before`), run ONLY the optimized job, compare to that run's
    recorded output (`anchor_after`). No synthetic data, no re-running the original."""
    verdict = flow.replay_validate(
        spark, job_name=job_name, opt_job_id=opt_job_id, catalog=catalog, src_schema=src_schema,
        tables=tables, targets=targets, cluster_id=cluster_id, anchor_before=anchor_before,
        anchor_after=anchor_after, table_pins=table_pins, target_pins=target_pins,
        partition_cols=partition_cols, risk_tier=risk_tier, run_timeout_s=run_timeout_s)
    _print(verdict)
    return verdict


def _print(verdict):
    print(f"flow_equivalence {'PASSED' if verdict['passed'] else 'FAILED'}")
    for t, r in verdict["comparison"]["targets"].items():
        print(f"  {t:20s}  {'pass' if r['passed'] else 'FAIL: ' + r.get('reason', '')[:120]}")
