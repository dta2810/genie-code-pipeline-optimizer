"""Performance benchmark harness: fair before/after timing + the perf gate.

v1 measures wall-clock (median of N with a discarded warmup). DBU/shuffle/spill attribution
comes from system.query.history / the query profile — pulled separately in perf-profile.
"""
import statistics
import time


def time_runs(fn, runs: int = 3, warmup: bool = True) -> dict:
    """Time a zero-arg callable `runs` times (median), discarding one warmup run."""
    if warmup:
        fn()
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return {"runs_s": times, "median_s": statistics.median(times),
            "min_s": min(times), "max_s": max(times)}


def benchmark(baseline_fn, candidate_fn, *, runs: int = 3, warmup: bool = True) -> dict:
    """Time baseline and candidate on the SAME session/compute (sandbox). Order: baseline first."""
    return {"baseline": time_runs(baseline_fn, runs, warmup),
            "candidate": time_runs(candidate_fn, runs, warmup)}


def perf_gate(before_median_s: float, after_median_s: float, min_gain: float = 0.20) -> dict:
    """Pass only if the candidate is faster by at least `min_gain` (fractional speedup)."""
    gain = 0.0 if not before_median_s else (before_median_s - after_median_s) / before_median_s
    return {"before_s": before_median_s, "after_s": after_median_s,
            "gain": gain, "min_gain": min_gain, "passed": gain >= min_gain}
