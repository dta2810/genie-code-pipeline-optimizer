"""Portable settings — resolve names from env vars, no hard-coded catalog."""
import os


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# Sandbox catalog: clones + remapped writes land here.
SANDBOX_CATALOG = _env("PO_SANDBOX_CATALOG", "opt_sandbox")
# Factory schema: audit table + config + governance views.
FACTORY_CATALOG = _env("PO_FACTORY_CATALOG", "main")
FACTORY_SCHEMA = _env("PO_FACTORY_SCHEMA", "pipeline_opt_factory")


def factory_fqn(name: str) -> str:
    """Fully-qualified name inside the factory schema."""
    return f"{FACTORY_CATALOG}.{FACTORY_SCHEMA}.{name}"


def sandbox_fqn(schema: str, table: str) -> str:
    """Map a prod schema.table onto the sandbox catalog (same schema/table)."""
    return f"{SANDBOX_CATALOG}.{schema}.{table}"
