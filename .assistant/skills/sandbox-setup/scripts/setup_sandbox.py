"""sandbox-setup: pin inputs + shallow-clone targets + remap writes. Wraps lib.sandbox."""
import os
import sys

# make the repo root importable (scripts/ -> skill/ -> skills/ -> .assistant/ -> repo root)
if "__file__" in globals():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 4)))

from lib.audit import audit_step  # noqa: E402
from lib.sandbox import clone_targets, pin_inputs, remap_writes  # noqa: E402


def setup(spark, *, job, notebook, source_tables, target_tables, notebook_src, suffix):
    """Build one isolated run environment. `suffix` (e.g. '_v1'/'_v2') makes independent clones.

    Returns {pins, clones, remap}. RAISES if any production write ref survives the remap.
    """
    with audit_step(spark, job=job, notebook=notebook, step="sandbox_setup"):
        pins = pin_inputs(spark, source_tables)
        clones = clone_targets(spark, target_tables, suffix=suffix)
        remap = remap_writes(notebook_src, clones)
        if not remap["safe"]:
            raise ValueError(f"Unsafe remap — prod refs remain: {remap['residual_prod_refs']}")
        return {"pins": pins, "clones": clones, "remap": remap}
