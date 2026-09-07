"""Provision the factory UC assets. Param-driven with defaults; callable or via 00_deploy widgets."""
import json
import os
import re

DEFAULTS = {
    "factory_catalog": "main",
    "factory_schema": "pipeline_opt_factory",
    "sandbox_schema": "pipeline_opt_sandbox",
}
# Mirror of lib.settings.CONFIG_FILE_TMPL — where the runtime harness reads the factory location
# (outside the bundle sync root, so `bundle deploy` never deletes it). Keep the two in sync.
RUNTIME_CONFIG_TMPL = "/Workspace/Users/{user}/.genie_optimizer_factory.json"
SQL_FILES = ["tables.sql", "config_function.sql", "governance_views.sql"]


def _sql_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sql"))


def _split_statements(sql: str) -> list[str]:
    """Split a .sql file into executable statements. Strips `--` line comments FIRST so a
    semicolon inside a comment never splits a statement (our DDL has no `--` inside literals),
    then splits on `;`.
    """
    no_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def provision(spark, *,
              factory_catalog: str = DEFAULTS["factory_catalog"],
              factory_schema: str = DEFAULTS["factory_schema"],
              sandbox_schema: str = DEFAULTS["sandbox_schema"],
              workspace_home: str | None = None,
              create_catalogs: bool = True,
              sql_dir: str | None = None) -> dict:
    """Create factory schema + tables + get_opt_config + governance views. Idempotent.

    Set create_catalogs=False if the factory catalog already exists or you lack CREATE CATALOG.
    The sandbox schema is NOT created here — it is created lazily inside each target's own
    catalog by lib.sandbox.clone_targets, so no extra catalog/privilege is needed. Also records the
    resolved config to the user's runtime config file so the harness needs no process env.
    """
    sql_dir = sql_dir or _sql_dir()
    if create_catalogs:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {factory_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {factory_catalog}.{factory_schema}")

    for fn in SQL_FILES:
        with open(os.path.join(sql_dir, fn)) as f:
            body = (f.read().replace("{{catalog}}", factory_catalog)
                            .replace("{{schema}}", factory_schema))
        for stmt in _split_statements(body):
            spark.sql(stmt)
        print(f"✓ {fn}")

    target = f"{factory_catalog}.{factory_schema}"
    print(f"✓ factory={target}  sandbox_schema={sandbox_schema} (created lazily in each target's catalog)")

    # Record the resolved config where the runtime harness (settings.configure) reads it.
    cfg = {"factory_catalog": factory_catalog, "factory_schema": factory_schema,
           "sandbox_schema": sandbox_schema}
    if workspace_home:
        cfg["workspace_home"] = workspace_home
    try:
        user = spark.sql("SELECT current_user()").collect()[0][0]
        path = RUNTIME_CONFIG_TMPL.format(user=user)
        with open(path, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"✓ runtime config -> {path}")
    except Exception as e:
        print(f"! could not write runtime config ({e}); set PO_FACTORY_* env or call "
              "settings.configure(...) at flow start")
    return {"factory": target, "sandbox_schema": sandbox_schema}
