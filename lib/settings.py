"""Portable settings. Runtime resolution order: explicit configure() > env vars > the optimizer
config file written at provision time (path derived from current_user) > defaults.

Env vars set on a laptop do NOT reach the Databricks runtime, so inside Genie Code the harness
learns the optimizer location either from an explicit configure(...) call or from the small config
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
# Optimizer schema: audit table + config + governance views.
OPTIMIZER_CATALOG = _env("PO_OPTIMIZER_CATALOG", "main")
OPTIMIZER_SCHEMA = _env("PO_OPTIMIZER_SCHEMA", "genie_optimizer")
# Workspace home: where the Genie Code assets live (skills, lib, deploy, sql, v2 notebooks).
WORKSPACE_HOME = _env("PO_WORKSPACE_HOME", "/Workspace/Users/<you>/genie_code_optimizer")
# Dedicated cluster for the HEAVY sandbox work (running v1/v2 notebooks + wall-clock benchmark).
# Governed per-job in opt_config.compute_cluster_id; this is the workspace-wide default. Empty ->
# the harness falls back to the serverless session (fine at sample scale, but noisy wall-clock).
COMPUTE_CLUSTER_ID = _env("PO_COMPUTE_CLUSTER_ID", "")

# Where provision records the resolved optimizer config so the runtime can pick it up without env.
# A dotfile in the user's workspace home — OUTSIDE the bundle sync root, so `bundle deploy` never
# deletes it. {user} is filled from current_user at runtime.
CONFIG_FILE_TMPL = "/Workspace/Users/{user}/.genie_optimizer.json"


def _current_user(spark):
    try:
        return spark.sql("SELECT current_user()").collect()[0][0]
    except Exception:
        return None


def _discover_optimizer(spark):
    """Find an already-provisioned optimizer by scanning for its `opt_config` table. Returns
    (catalog, schema) only when exactly ONE exists (unambiguous) — so a session adopts the
    deployed optimizer instead of provisioning a duplicate at the defaults.
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


def configure(*, spark=None, optimizer_catalog=None, optimizer_schema=None,
              sandbox_schema=None, workspace_home=None, compute_cluster_id=None) -> dict:
    """Set the optimizer location for this runtime. Explicit args win; otherwise, if `spark` is
    given, derive the workspace home from current_user and load optimizer_catalog/schema/sandbox/
    compute from the provision-written config file (if present). Idempotent. Call once at flow start.
    """
    global OPTIMIZER_CATALOG, OPTIMIZER_SCHEMA, SANDBOX_SCHEMA, WORKSPACE_HOME, COMPUTE_CLUSTER_ID
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
        # No explicit/file optimizer -> adopt an existing one instead of falling to defaults.
        if not optimizer_catalog and "optimizer_catalog" not in loaded:
            d = _discover_optimizer(spark)
            if d:
                disc = {"optimizer_catalog": d[0], "optimizer_schema": d[1]}
    OPTIMIZER_CATALOG = optimizer_catalog or loaded.get("optimizer_catalog") or disc.get("optimizer_catalog") or OPTIMIZER_CATALOG
    OPTIMIZER_SCHEMA = optimizer_schema or loaded.get("optimizer_schema") or disc.get("optimizer_schema") or OPTIMIZER_SCHEMA
    SANDBOX_SCHEMA = sandbox_schema or loaded.get("sandbox_schema", SANDBOX_SCHEMA)
    WORKSPACE_HOME = workspace_home or loaded.get("workspace_home", WORKSPACE_HOME)
    COMPUTE_CLUSTER_ID = compute_cluster_id or loaded.get("compute_cluster_id", COMPUTE_CLUSTER_ID)
    return resolved()


def resolved() -> dict:
    """The optimizer location currently in effect."""
    return {"optimizer_catalog": OPTIMIZER_CATALOG, "optimizer_schema": OPTIMIZER_SCHEMA,
            "sandbox_schema": SANDBOX_SCHEMA, "workspace_home": WORKSPACE_HOME,
            "compute_cluster_id": COMPUTE_CLUSTER_ID}


def optimizer_fqn(name: str) -> str:
    """Fully-qualified name inside the optimizer schema."""
    return f"{OPTIMIZER_CATALOG}.{OPTIMIZER_SCHEMA}.{name}"


import re as _re


def _safe(name: str) -> str:
    """A workspace-path-safe folder name for a job (spaces/specials -> _)."""
    return _re.sub(r"[^0-9A-Za-z._-]+", "_", (name or "job").strip()).strip("_") or "job"


def job_home(job_name: str) -> str:
    """Per-job workspace folder: everything the optimizer produces for a job lives here."""
    return f"{WORKSPACE_HOME.rstrip('/')}/jobs/{_safe(job_name)}"


def optimized_folder(job_name: str) -> str:
    """Folder for generated v2 candidate notebooks: `<WORKSPACE_HOME>/jobs/<job>/optimized`.
    `job_name` is REQUIRED — there is no shared top-level folder (it would strand notebooks
    outside the per-job layout, which is exactly how a stray `optimized/<other-job>` appeared)."""
    if not job_name:
        raise ValueError("optimized_folder requires a job_name — no shared top-level folder "
                         "(everything a job produces lives under job_home(job_name)).")
    return f"{job_home(job_name)}/optimized"


def validation_folder(job_name: str) -> str:
    """Folder for the dedicated per-notebook validation notebooks (clone+run+equivalence+benchmark,
    one operation per cell) — run on the dedicated cluster, never inline in the chat turn."""
    return f"{job_home(job_name)}/validation"


def driver_folder(job_name: str) -> str:
    """Home for the Genie Code driver/orchestrator notebook + run traceability:
    `<WORKSPACE_HOME>/jobs/<job>/driver`. Keeps the interactive notebook INSIDE the framework home
    instead of a stray agent folder (e.g. `agents_governance/`), so every artifact is under one root."""
    return f"{job_home(job_name)}/driver"


def assert_under_home(path: str) -> str:
    """Guard: every workspace artifact the framework writes must live under WORKSPACE_HOME. Raises
    on a path outside it (e.g. a hard-coded or agent-default location). Returns the path if OK."""
    home = WORKSPACE_HOME.rstrip("/")
    if not (path == home or path.startswith(home + "/")):
        raise ValueError(f"{path} is outside WORKSPACE_HOME ({home}) — use a settings folder helper "
                         "(optimized_folder/validation_folder/driver_folder); never hard-code a path.")
    return path


def sandbox_fqn(catalog: str, schema: str, table: str) -> str:
    """Map a prod catalog.schema.table onto the sandbox schema, inside the SAME catalog.

    Every clone lands in one dedicated sandbox schema; the original schema is folded into the
    table name (`<origschema>__<table>`) so tables from different prod schemas never collide.
    """
    return f"{catalog}.{SANDBOX_SCHEMA}.{schema}__{table}"
