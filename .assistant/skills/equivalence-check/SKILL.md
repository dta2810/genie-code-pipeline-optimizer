---
name: equivalence-check
description: Prove a candidate notebook produces the SAME result as the baseline (before == after), step by step. Runs cheapest-first checks — row counts, per-column fingerprints, then EXCEPT ALL both directions with epsilon tolerance for floats. Localizes divergence per intermediate step. HARD gate — if it fails, the candidate is NOT promoted.
---

# equivalence-check

Import `scripts/check_equivalence.py` — do NOT reimplement inline. The baseline is the reference
truth; you prove the candidate is equivalent **to it**.

Inputs: baseline output(s) + candidate output(s) (from the sandbox), `equivalence_keys`,
`risk_tier` (the technique applied, from optimization-catalog), and optionally `epsilon`.

`risk_tier` sets the epsilon policy when `epsilon` is not given (see optimization-catalog):
- **zero_risk** (broadcast/clustering/small-files) → **exact** float compare (epsilon 0); any drift
  is a bug, not reordering.
- **semantics_preserving** (de-UDF, salting) → epsilon tolerance; the full `EXCEPT ALL` is
  mandatory and unsampled (float reassociation is expected, wrong rows are not).
- **refresh_change** (incremental MV) → epsilon tolerance; pass `baseline_is_full_recompute=True`
  and compare the incremental result against a **full recompute over the same pinned inputs** — the
  check refuses to certify otherwise.

Checks (cheapest first; stop early on divergence):
1. **Schema** — same columns/types (allow benign widening if the config permits).
2. **Counts** — total and per-partition row counts equal.
3. **Column fingerprint** — sum / min / max / count-distinct / null-count per column match.
   This is a **pre-filter, never proof** — it never substitutes for EXCEPT ALL.
4. **Hard proof** — `baseline EXCEPT ALL candidate` AND `candidate EXCEPT ALL baseline`, both
   empty (order- and duplicate-safe). Exact floats for zero_risk; round DECIMAL/DOUBLE to `epsilon`
   otherwise.
5. **Step-by-step** — repeat per materialized intermediate so divergence is localized.

On divergence: `audit_log(step="equivalence", status="failed", insight=<where + why it diverged>)`
and RAISE. Never soften `epsilon` to force a pass. The audit records the `risk_tier` and `epsilon`.
