"""Portable settings — resolve names from env vars, no hard-coded catalog."""
import os


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# Sandbox schema: a dedicated schema (inside prod's own catalog) where clones + remapped
# writes land. Schema-based (not catalog-based) so isolation needs no CREATE CATALOG privilege.
SANDBOX_SCHEMA = _env("PO_SANDBOX_SCHEMA", "pipeline_opt_sandbox")
# Factory schema: audit table + config + governance views.
FACTORY_CATALOG = _env("PO_FACTORY_CATALOG", "main")
FACTORY_SCHEMA = _env("PO_FACTORY_SCHEMA", "pipeline_opt_factory")
# Workspace home: where the Genie Code assets live (skills, lib, deploy, sql, v2 notebooks).
WORKSPACE_HOME = _env("PO_WORKSPACE_HOME", "/Workspace/Users/<you>/genie_code_optimizer")


def factory_fqn(name: str) -> str:
    """Fully-qualified name inside the factory schema."""
    return f"{FACTORY_CATALOG}.{FACTORY_SCHEMA}.{name}"


def optimized_folder() -> str:
    """Workspace folder for generated v2 notebooks (derives from WORKSPACE_HOME)."""
    return f"{WORKSPACE_HOME.rstrip('/')}/optimized"


def sandbox_fqn(catalog: str, schema: str, table: str) -> str:
    """Map a prod catalog.schema.table onto the sandbox schema, inside the SAME catalog.

    Every clone lands in one dedicated sandbox schema; the original schema is folded into the
    table name (`<origschema>__<table>`) so tables from different prod schemas never collide.
    """
    return f"{catalog}.{SANDBOX_SCHEMA}.{schema}__{table}"
