"""Provision the factory UC assets. Param-driven with defaults; callable or via 00_deploy widgets."""
import os

DEFAULTS = {
    "factory_catalog": "main",
    "factory_schema": "pipeline_opt_factory",
    "sandbox_schema": "pipeline_opt_sandbox",
}
SQL_FILES = ["tables.sql", "config_function.sql", "governance_views.sql"]


def _sql_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sql"))


def provision(spark, *,
              factory_catalog: str = DEFAULTS["factory_catalog"],
              factory_schema: str = DEFAULTS["factory_schema"],
              sandbox_schema: str = DEFAULTS["sandbox_schema"],
              create_catalogs: bool = True,
              sql_dir: str | None = None) -> dict:
    """Create factory schema + tables + get_opt_config + governance views. Idempotent.

    Set create_catalogs=False if the factory catalog already exists or you lack CREATE CATALOG.
    The sandbox schema is NOT created here — it is created lazily inside each target's own
    catalog by lib.sandbox.clone_targets, so no extra catalog/privilege is needed.
    """
    sql_dir = sql_dir or _sql_dir()
    if create_catalogs:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {factory_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {factory_catalog}.{factory_schema}")

    for fn in SQL_FILES:
        with open(os.path.join(sql_dir, fn)) as f:
            body = (f.read().replace("{{catalog}}", factory_catalog)
                            .replace("{{schema}}", factory_schema))
        for stmt in [s.strip() for s in body.split(";") if s.strip()]:
            spark.sql(stmt)
        print(f"✓ {fn}")

    target = f"{factory_catalog}.{factory_schema}"
    print(f"✓ factory={target}  sandbox_schema={sandbox_schema} (created lazily in each target's catalog)")
    return {"factory": target, "sandbox_schema": sandbox_schema}
