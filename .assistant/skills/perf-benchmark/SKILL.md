---
name: perf-benchmark
description: Measure baseline vs candidate performance fairly and decide the perf gate. Runs both on the same compute against the sandbox, controls warm/cold cache, takes the median of N runs, and reports wall-clock, DBU/cost, shuffle, and spill. Promote only if the candidate is equivalent AND faster/cheaper by min_gain.
---

# perf-benchmark

Import `scripts/benchmark_perf.py` — do NOT reimplement inline.

Inputs: baseline notebook + candidate notebook, `compute`, `benchmark_runs`, `min_gain`.

**Equivalence is the proof; perf is a separate, advisory reading — keep them apart.** A candidate is
correct because equivalence passed, NOT because it was faster. Report the two independently.

**Serverless warning:** wall-clock on serverless is contaminated by cold starts / autoscale — a
tiny sampled run can show minutes of "runtime" that is overhead, not I/O (e.g. a 500K-row rewrite
timing 1259s). Do NOT quote such a number as the speedup. Trust the perf number only from a **warm,
dedicated cluster**; on serverless, report the I/O pattern (rows/files rewritten: full-rewrite vs
delta-touch) as the evidence and label wall-clock "indicative only".

Steps:
1. Run baseline and candidate on the **same** cluster/warehouse against the sandbox clones.
2. Control cache: warm-up run discarded, then `benchmark_runs` measured; take the **median**.
3. Capture wall-clock + DBU/cost + Spark metrics (shuffle read/write, spill, bytes/files scanned).
   Also record the structural signal: rows/files read+written by v1 vs v2 (this survives serverless
   noise and is often the more honest story — e.g. rewrite 180M vs touch 1.5M).
4. Perf gate (**advisory, not a hard block** — see optimize-pipeline): report gain vs `min_gain`;
   below-threshold is a signal, and a correctness/maintainability promotion is recorded as such.
5. `audit_log(step="perf_benchmark", insight=...)` with before/after + structural I/O + gate outcome,
   flagging whether the timing was on dedicated compute or indicative-only on serverless.

Output: perf verdict (advisory) feeding the promotion decision, separate from the equivalence proof.
