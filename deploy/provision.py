"""Provision the optimizer UC assets. Param-driven with defaults; callable or via 00_deploy widgets."""
import json
import os
import re

DEFAULTS = {
    "optimizer_catalog": "main",
    "optimizer_schema": "genie_optimizer",
    "sandbox_schema": "pipeline_opt_sandbox",
    "compute_cluster_id": "",
}
# Columns added to opt_config after its first release — ALTERed in idempotently so redeploys pick
# them up on an existing table (CREATE TABLE IF NOT EXISTS never adds a column). name -> type.
OPT_CONFIG_ADDED_COLUMNS = {"compute_cluster_id": "STRING"}
# Mirror of lib.settings.CONFIG_FILE_TMPL — where the runtime harness reads the optimizer location
# (outside the bundle sync root, so `bundle deploy` never deletes it). Keep the two in sync.
RUNTIME_CONFIG_TMPL = "/Workspace/Users/{user}/.genie_optimizer.json"
SQL_FILES = ["tables.sql", "config_function.sql", "governance_views.sql"]


def _sql_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sql"))


def _default_yaml() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "optimizer.yaml"))


def load_yaml_config(path: str | None = None) -> dict:
    """Read the deploy config (config/optimizer.yaml by default). Empty dict if the file is absent."""
    import yaml
    path = path or _default_yaml()
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def deploy_from_yaml(spark, path: str | None = None) -> dict:
    """One-command deploy driven by config/optimizer.yaml. Runs entirely in Databricks (spark).

    Manual:      open deploy/00_deploy.py and Run All.
    Genie Code:  from provision import deploy_from_yaml; deploy_from_yaml(spark)
    """
    cfg = load_yaml_config(path)
    # An empty yaml scalar (e.g. `compute_cluster_id:`) parses to None; `or DEFAULTS[...]` keeps it
    # from becoming the string "None" downstream (which would break the serverless fallback).
    def _s(key):
        return cfg.get(key) or DEFAULTS[key]
    return provision(
        spark,
        optimizer_catalog=_s("optimizer_catalog"),
        optimizer_schema=_s("optimizer_schema"),
        sandbox_schema=_s("sandbox_schema"),
        compute_cluster_id=_s("compute_cluster_id"),
        workspace_home=cfg.get("workspace_home") or None,
        create_catalogs=bool(cfg.get("create_catalogs", False)),
    )


def _split_statements(sql: str) -> list[str]:
    """Split a .sql file into executable statements. Strips `--` line comments FIRST so a
    semicolon inside a comment never splits a statement (our DDL has no `--` inside literals),
    then splits on `;`.
    """
    no_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def provision(spark, *,
              optimizer_catalog: str = DEFAULTS["optimizer_catalog"],
              optimizer_schema: str = DEFAULTS["optimizer_schema"],
              sandbox_schema: str = DEFAULTS["sandbox_schema"],
              compute_cluster_id: str = DEFAULTS["compute_cluster_id"],
              workspace_home: str | None = None,
              create_catalogs: bool = True,
              sql_dir: str | None = None) -> dict:
    """Create optimizer schema + tables + get_opt_config + governance views. Idempotent.

    Set create_catalogs=False if the optimizer catalog already exists or you lack CREATE CATALOG.
    The sandbox schema is NOT created here — it is created lazily inside each target's own
    catalog by lib.sandbox.clone_targets, so no extra catalog/privilege is needed. Also records the
    resolved config to the user's runtime config file so the harness needs no process env.
    """
    sql_dir = sql_dir or _sql_dir()
    # Normalize a missing cluster to "" so resolve_compute falls back to serverless (a stray "None"
    # string — e.g. from str(None) in a widget prefill — would otherwise look like a real cluster id).
    if str(compute_cluster_id).strip().lower() in ("", "none"):
        compute_cluster_id = ""

    # Guard against a duplicate optimizer: if opt_config already exists elsewhere, point at it.
    target = f"{optimizer_catalog}.{optimizer_schema}"
    try:
        existing = [f"{r['table_catalog']}.{r['table_schema']}" for r in spark.sql(
            "SELECT table_catalog, table_schema FROM system.information_schema.tables "
            "WHERE table_name = 'opt_config'").collect()]
        other = [e for e in existing if e != target]
        if other:
            print(f"! A optimizer already exists at {other} — NOT creating a duplicate at {target}. "
                  f"Use settings.configure() to adopt it (it auto-discovers), or pass that schema.")
            return {"optimizer": other[0], "sandbox_schema": sandbox_schema, "skipped": True}
    except Exception:
        pass

    if create_catalogs:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {optimizer_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {optimizer_catalog}.{optimizer_schema}")

    for fn in SQL_FILES:
        with open(os.path.join(sql_dir, fn)) as f:
            body = (f.read().replace("{{catalog}}", optimizer_catalog)
                            .replace("{{schema}}", optimizer_schema))
        for stmt in _split_statements(body):
            spark.sql(stmt)
        print(f"✓ {fn}")
        if fn == "tables.sql":  # evolve an existing opt_config before get_opt_config reads new cols
            existing_cols = {r["column_name"] for r in spark.sql(
                f"SELECT column_name FROM {optimizer_catalog}.information_schema.columns "
                f"WHERE table_schema = '{optimizer_schema}' AND table_name = 'opt_config'").collect()}
            for col, typ in OPT_CONFIG_ADDED_COLUMNS.items():
                if col not in existing_cols:  # no ADD COLUMN IF NOT EXISTS on this runtime
                    spark.sql(f"ALTER TABLE {optimizer_catalog}.{optimizer_schema}.opt_config "
                              f"ADD COLUMNS ({col} {typ})")
            print(f"✓ opt_config columns ensured: {list(OPT_CONFIG_ADDED_COLUMNS)}")

    target = f"{optimizer_catalog}.{optimizer_schema}"
    print(f"✓ optimizer={target}  sandbox_schema={sandbox_schema} (created lazily in each target's catalog)")

    # Record the resolved config where the runtime harness (settings.configure) reads it.
    cfg = {"optimizer_catalog": optimizer_catalog, "optimizer_schema": optimizer_schema,
           "sandbox_schema": sandbox_schema}
    if compute_cluster_id:
        cfg["compute_cluster_id"] = compute_cluster_id
    if workspace_home:
        cfg["workspace_home"] = workspace_home
    try:
        user = spark.sql("SELECT current_user()").collect()[0][0]
        path = RUNTIME_CONFIG_TMPL.format(user=user)
        with open(path, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"✓ runtime config -> {path}")
    except Exception as e:
        print(f"! could not write runtime config ({e}); set PO_OPTIMIZER_* env or call "
              "settings.configure(...) at flow start")
    return {"optimizer": target, "sandbox_schema": sandbox_schema}
