---
name: equivalence-check
description: Prove a candidate notebook produces the SAME result as the baseline (before == after), step by step. Runs cheapest-first checks — row counts, per-column fingerprints, then EXCEPT ALL both directions with epsilon tolerance for floats. Localizes divergence per intermediate step. HARD gate — if it fails, the candidate is NOT promoted.
---

# equivalence-check

Import `scripts/check_equivalence.py` — do NOT reimplement inline. The baseline is the reference
truth; you prove the candidate is equivalent **to it**.

Inputs: baseline output(s) + candidate output(s) (from the sandbox), `equivalence_keys`, `epsilon`.

Checks (cheapest first; stop early on divergence):
1. **Schema** — same columns/types (allow benign widening if the config permits).
2. **Counts** — total and per-partition row counts equal.
3. **Column fingerprint** — sum / min / max / count-distinct / null-count per column match.
4. **Hard proof** — `baseline EXCEPT ALL candidate` AND `candidate EXCEPT ALL baseline`, both
   empty (order- and duplicate-safe). Round DECIMAL/DOUBLE to `epsilon` first.
5. **Step-by-step** — repeat per materialized intermediate so divergence is localized.

On divergence: `audit_log(step="equivalence", status="failed", insight=<where + why it diverged>)`
and RAISE. Never soften `epsilon` to force a pass.
