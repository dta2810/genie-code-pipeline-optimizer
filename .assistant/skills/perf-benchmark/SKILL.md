---
name: perf-benchmark
description: Measure baseline vs candidate performance fairly and decide the perf gate. Runs both on the same compute against the sandbox, controls warm/cold cache, takes the median of N runs, and reports wall-clock, DBU/cost, shuffle, and spill. Promote only if the candidate is equivalent AND faster/cheaper by min_gain.
---

# perf-benchmark

Import `scripts/benchmark_perf.py` — do NOT reimplement inline.

Inputs: baseline notebook + candidate notebook, `compute`, `benchmark_runs`, `min_gain`.

Steps:
1. Run baseline and candidate on the **same** cluster/warehouse against the sandbox clones.
2. Control cache: warm-up run discarded, then `benchmark_runs` measured; take the **median**.
3. Capture wall-clock + DBU/cost + Spark metrics (shuffle read/write, spill, bytes/files scanned).
4. Perf gate: pass only if `runtime_after <= runtime_before * (1 - min_gain)` with no cost
   regression.
5. `audit_log(step="perf_benchmark", insight=...)` with before/after + the gate outcome.

Output: perf verdict feeding the champion/challenger promotion decision.
