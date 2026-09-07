---
name: optimize-notebook
description: Generate an optimized v2 of a notebook after the human approves the suggestions. Writes a NEW notebook named <ntb>_genie_opt_<timestamp> in the dedicated optimized_folder, applying the approved techniques from optimization-catalog (broadcast/skew/de-UDF/clustering/small-files/incremental-MV). Never edits the original. Writes are already remapped to the sandbox by sandbox-setup.
---

# optimize-notebook

Import `scripts/generate_v2.py` — do NOT reimplement inline. Requires human approval of the
suggestions (GATE 1) BEFORE running.

Inputs: original notebook, approved suggestions (each names a technique), `optimized_folder`.

Steps:
1. Copy the original into `<optimized_folder>/<ntb>_genie_opt_<timestamp>` — never edit the source.
2. For each approved technique, load ONLY its `optimization-catalog/resources/<technique>.md` and
   apply that recipe — do NOT reimplement the transformation inline.
3. Apply one transformation per logical step, keeping intermediate outputs materializable for
   step-by-step equivalence. Honor the technique's equivalence-risk tier (semantics-preserving
   rewrites like de-UDF/salting demand a full, unsampled gate downstream).
4. Keep the write targets as configured so `sandbox-setup` remaps them to the sandbox.
5. `audit_log(step="generate_v2", change_type=<list of techniques>, insight=<what changed + why faster>)`.

Output: the v2 notebook path, ready for sandbox-setup → run → equivalence-check → perf-benchmark.
