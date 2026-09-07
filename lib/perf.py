"""Performance benchmark harness: fair before/after timing + the perf gate.

v1 measures wall-clock (median of N with a discarded warmup). DBU/shuffle/spill attribution
comes from system.query.history / the query profile — pulled separately in perf-profile.
"""
import statistics
import time


def rank_notebooks(spark, job_id, notebooks: list[dict], *, lookback_days: int = 30) -> dict:
    """Cheap, read-only job-level ranking: which notebook is the biggest cost hotspot.

    Attributes runtime x frequency per task from `system.lakeflow.job_task_run_timeline` over the
    last `lookback_days`. Score = total compute seconds (runtime x runs); on shared job compute this
    tracks DBU. Read-only — no writes, negligible cost. This is the LIGHT pass that drives the
    selection recommendation; `perf-profile` does the DEEP root-cause dive on the chosen notebook.
    Falls back gracefully (`ranked=False`) if the system table is unavailable or has no rows, so the
    orchestrator can ask the user to pick instead. Needs `task_key` on each notebook (from bootstrap).
    """
    try:
        rows = spark.sql(f"""
            SELECT task_key,
                   COUNT(DISTINCT run_id) AS runs,
                   SUM(unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) AS total_s
            FROM system.lakeflow.job_task_run_timeline
            WHERE job_id = {int(job_id)}
              AND period_start_time >= current_timestamp() - INTERVAL {int(lookback_days)} DAYS
              AND period_end_time IS NOT NULL
            GROUP BY task_key
        """).collect()
        by_task = {r["task_key"]: {"runs": int(r["runs"] or 0), "total_s": float(r["total_s"] or 0.0)}
                   for r in rows}
    except Exception as e:  # table missing / no perms / etc. -> let the human pick
        return {"ranked": False, "reason": f"no telemetry ({e})", "notebooks": notebooks}

    total = sum(v["total_s"] for v in by_task.values())
    ranked = []
    for n in notebooks:
        m = by_task.get(n.get("task_key"), {"runs": 0, "total_s": 0.0})
        ranked.append({**n, "runs": m["runs"], "total_runtime_s": m["total_s"],
                       "cost_share": (m["total_s"] / total) if total else 0.0})
    ranked.sort(key=lambda x: x["total_runtime_s"], reverse=True)
    recommended = ranked[0] if ranked and ranked[0]["total_runtime_s"] > 0 else None
    return {"ranked": total > 0, "lookback_days": lookback_days, "total_runtime_s": total,
            "notebooks": ranked, "recommended": recommended}


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
