"""Isolated sandbox: pin inputs, shallow-clone targets, remap writes. Never touch prod."""
import re

from . import settings
from .settings import sandbox_fqn


def _parts(fqn: str):
    p = fqn.replace("`", "").split(".")
    if len(p) != 3:
        raise ValueError(f"Expected catalog.schema.table, got {fqn!r}")
    return p  # [catalog, schema, table]


def pin_inputs(spark, source_tables: list[str]) -> dict[str, int]:
    """Latest committed Delta version per source, for reproducible `VERSION AS OF` reads."""
    versions = {}
    for t in source_tables:
        versions[t] = int(spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").collect()[0]["version"])
    return versions


def clone_targets(spark, target_tables: list[str], suffix: str = "") -> dict[str, str]:
    """Shallow-clone each target into the sandbox schema WITH current data (needed for MERGE).

    The sandbox schema lives inside the target's OWN catalog, so no CREATE CATALOG privilege
    is needed. `suffix` lets the caller make independent clones per run (e.g. "_v1"/"_v2") from
    the same source state, so baseline and candidate each write into a fresh copy.
    """
    mapping = {}
    for t in target_tables:
        catalog, schema, table = _parts(t)
        dst = sandbox_fqn(catalog, schema, f"{table}{suffix}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{settings.SANDBOX_SCHEMA}")
        spark.sql(f"CREATE OR REPLACE TABLE {dst} SHALLOW CLONE {t}")
        mapping[t] = dst
    return mapping


def remap_writes(notebook_src: str, clone_map: dict[str, str]) -> dict:
    """Rewrite known prod table refs to their sandbox clones; verify none remain. Pure transform.

    Uses exact FQN boundaries (not code parsing) — we already know the precise targets from
    detect-tables, so this is a targeted, safe substitution.
    """
    src, replacements = notebook_src, {}
    for prod, dst in clone_map.items():
        pat = re.compile(rf"(?<![\w.]){re.escape(prod)}(?![\w])")
        src, n = pat.subn(dst, src)
        replacements[prod] = n
    residual = [p for p in clone_map
                if re.search(rf"(?<![\w.]){re.escape(p)}(?![\w])", src)]
    return {"source": src, "replacements": replacements,
            "residual_prod_refs": residual, "safe": not residual}
