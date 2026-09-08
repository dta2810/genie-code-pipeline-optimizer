"""Portable settings. Runtime resolution order: explicit configure() > env vars > the factory
config file written at provision time (path derived from current_user) > defaults.

Env vars set on a laptop do NOT reach the Databricks runtime, so inside Genie Code the harness
learns the factory location either from an explicit configure(...) call or from the small config
file that provision writes to the user's workspace home. Consumers read `settings.X` (or the helper
functions) at CALL time — never `from .settings import X` — so configure() takes effect everywhere.
"""
import json
import os


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# Sandbox schema: a dedicated schema (inside prod's own catalog) where clones + remapped writes
# land. Schema-based (not catalog-based) so isolation needs no CREATE CATALOG privilege.
SANDBOX_SCHEMA = _env("PO_SANDBOX_SCHEMA", "pipeline_opt_sandbox")
# Factory schema: audit table + config + governance views.
FACTORY_CATALOG = _env("PO_FACTORY_CATALOG", "main")
FACTORY_SCHEMA = _env("PO_FACTORY_SCHEMA", "pipeline_opt_factory")
# Workspace home: where the Genie Code assets live (skills, lib, deploy, sql, v2 notebooks).
WORKSPACE_HOME = _env("PO_WORKSPACE_HOME", "/Workspace/Users/<you>/genie_code_optimizer")
# Dedicated cluster for the HEAVY sandbox work (running v1/v2 notebooks + wall-clock benchmark).
# Governed per-job in opt_config.compute_cluster_id; this is the workspace-wide default. Empty ->
# the harness falls back to the serverless session (fine at sample scale, but noisy wall-clock).
COMPUTE_CLUSTER_ID = _env("PO_COMPUTE_CLUSTER_ID", "")

# Where provision records the resolved factory config so the runtime can pick it up without env.
# A dotfile in the user's workspace home — OUTSIDE the bundle sync root, so `bundle deploy` never
# deletes it. {user} is filled from current_user at runtime.
CONFIG_FILE_TMPL = "/Workspace/Users/{user}/.genie_optimizer_factory.json"


def _current_user(spark):
    try:
        return spark.sql("SELECT current_user()").collect()[0][0]
    except Exception:
        return None


def _discover_factory(spark):
    """Find an already-provisioned factory by scanning for its `opt_config` table. Returns
    (catalog, schema) only when exactly ONE exists (unambiguous) — so a session adopts the
    deployed factory instead of provisioning a duplicate at the defaults.
    """
    try:
        rows = spark.sql("SELECT table_catalog, table_schema FROM system.information_schema.tables "
                         "WHERE table_name = 'opt_config'").collect()
    except Exception:
        return None
    cands = [(r["table_catalog"], r["table_schema"]) for r in rows]
    return cands[0] if len(cands) == 1 else None


def config_file_path(user: str) -> str:
    return CONFIG_FILE_TMPL.format(user=user)


def configure(*, spark=None, factory_catalog=None, factory_schema=None,
              sandbox_schema=None, workspace_home=None, compute_cluster_id=None) -> dict:
    """Set the factory location for this runtime. Explicit args win; otherwise, if `spark` is
    given, derive the workspace home from current_user and load factory_catalog/schema/sandbox/
    compute from the provision-written config file (if present). Idempotent. Call once at flow start.
    """
    global FACTORY_CATALOG, FACTORY_SCHEMA, SANDBOX_SCHEMA, WORKSPACE_HOME, COMPUTE_CLUSTER_ID
    loaded, disc = {}, {}
    if spark is not None:
        user = _current_user(spark)
        if user:
            if "<you>" in WORKSPACE_HOME and workspace_home is None:
                WORKSPACE_HOME = f"/Workspace/Users/{user}/genie_code_optimizer"
            try:
                with open(config_file_path(user)) as f:
                    loaded = json.load(f)
            except Exception:
                loaded = {}
        # No explicit/file factory -> adopt an existing one instead of falling to defaults.
        if not factory_catalog and "factory_catalog" not in loaded:
            d = _discover_factory(spark)
            if d:
                disc = {"factory_catalog": d[0], "factory_schema": d[1]}
    FACTORY_CATALOG = factory_catalog or loaded.get("factory_catalog") or disc.get("factory_catalog") or FACTORY_CATALOG
    FACTORY_SCHEMA = factory_schema or loaded.get("factory_schema") or disc.get("factory_schema") or FACTORY_SCHEMA
    SANDBOX_SCHEMA = sandbox_schema or loaded.get("sandbox_schema", SANDBOX_SCHEMA)
    WORKSPACE_HOME = workspace_home or loaded.get("workspace_home", WORKSPACE_HOME)
    COMPUTE_CLUSTER_ID = compute_cluster_id or loaded.get("compute_cluster_id", COMPUTE_CLUSTER_ID)
    return resolved()


def resolved() -> dict:
    """The factory location currently in effect."""
    return {"factory_catalog": FACTORY_CATALOG, "factory_schema": FACTORY_SCHEMA,
            "sandbox_schema": SANDBOX_SCHEMA, "workspace_home": WORKSPACE_HOME,
            "compute_cluster_id": COMPUTE_CLUSTER_ID}


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
