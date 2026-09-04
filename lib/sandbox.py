"""Isolated sandbox: pin inputs, shallow-clone targets, remap writes. Never touch prod."""
from .settings import sandbox_fqn


def pin_inputs(spark, source_tables: list[str]) -> dict[str, int]:
    """Resolve each source to its latest committed Delta version (for time-travel reads)."""
    # TODO: DESCRIBE HISTORY <t> LIMIT 1 -> version; return {table: version}.
    raise NotImplementedError


def clone_targets(spark, target_tables: list[str]) -> dict[str, str]:
    """Shallow-clone each target into the sandbox catalog WITH current data (needed for MERGE)."""
    # for t in target_tables:
    #   CREATE OR REPLACE TABLE <sandbox> SHALLOW CLONE <prod>
    # return {prod_table: sandbox_table}
    raise NotImplementedError


def remap_writes(notebook_src: str, target_tables: list[str]) -> str:
    """Rewrite write targets in the v2 notebook to the sandbox catalog; verify none hit prod."""
    raise NotImplementedError
