"""perf-benchmark: fair before/after timing + perf gate, audited. Wraps lib.perf."""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 4)))

from lib.audit import audit_log  # noqa: E402
from lib.perf import benchmark, perf_gate  # noqa: E402


def run(spark, *, job, notebook, baseline_fn, candidate_fn, runs=3, min_gain=0.20):
    """Benchmark both callables on the same compute; gate on min_gain; audit the verdict."""
    b = benchmark(baseline_fn, candidate_fn, runs=runs)
    gate = perf_gate(b["baseline"]["median_s"], b["candidate"]["median_s"], min_gain)
    audit_log(spark, job=job, notebook=notebook, step="perf_benchmark",
              status="succeeded" if gate["passed"] else "failed",
              perf={"before_s": round(gate["before_s"], 2), "after_s": round(gate["after_s"], 2),
                    "gain": round(gate["gain"], 4), "passed": gate["passed"]},
              insight=f"median {gate['before_s']:.1f}s -> {gate['after_s']:.1f}s "
                      f"({gate['gain'] * 100:.0f}% gain, gate {min_gain * 100:.0f}%)")
    return {"benchmark": b, "gate": gate}
