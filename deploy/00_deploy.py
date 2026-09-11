# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Deploy optimizer assets
# MAGIC Provisions the Unity Catalog assets the Genie Code Pipeline Optimizer needs:
# MAGIC schema, `opt_config`, `optimization_audit`, `get_opt_config`, and the governance views.
# MAGIC
# MAGIC Widgets pre-fill from `config/optimizer.yaml` — edit that file (or the widgets), then Run All.
# MAGIC Idempotent (`CREATE ... IF NOT EXISTS` / `OR REPLACE`). Logic lives in `provision.py`, so this
# MAGIC is equivalent to `from provision import deploy_from_yaml; deploy_from_yaml(spark)` (also how
# MAGIC Genie Code launches it).

# COMMAND ----------

from provision import DEFAULTS, load_yaml_config, provision

# Pre-fill widget defaults from config/optimizer.yaml when present (falls back to code DEFAULTS).
_y = load_yaml_config()
def _d(k):
    v = _y.get(k, DEFAULTS.get(k, ""))
    return "" if v is None else str(v)  # empty yaml scalar -> "" (not the string "None")

dbutils.widgets.text("optimizer_catalog", _d("optimizer_catalog"), "Optimizer catalog")
dbutils.widgets.text("optimizer_schema", _d("optimizer_schema"), "Optimizer schema")
dbutils.widgets.text("sandbox_schema", _d("sandbox_schema"), "Sandbox schema")
dbutils.widgets.text("compute_cluster_id", _d("compute_cluster_id"), "Dedicated cluster id (heavy runs)")
dbutils.widgets.text("workspace_home", str(_y.get("workspace_home", "")), "Workspace home")
dbutils.widgets.dropdown("create_catalogs", "true" if _y.get("create_catalogs", False) else "false",
                         ["true", "false"], "Create catalogs?")

# COMMAND ----------

info = provision(
    spark,
    optimizer_catalog=dbutils.widgets.get("optimizer_catalog"),
    optimizer_schema=dbutils.widgets.get("optimizer_schema"),
    sandbox_schema=dbutils.widgets.get("sandbox_schema"),
    compute_cluster_id=dbutils.widgets.get("compute_cluster_id"),
    workspace_home=dbutils.widgets.get("workspace_home") or None,
    create_catalogs=dbutils.widgets.get("create_catalogs") == "true",
)

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

fc, fs = info["optimizer"].split(".")
# information_schema is portable (SHOW VIEWS IN catalog.schema isn't supported on serverless).
display(spark.sql(
    f"SELECT table_name, table_type FROM {fc}.information_schema.tables "
    f"WHERE table_schema = '{fs}' ORDER BY table_type, table_name"))
print(f"Optimizer provisioned at {info['optimizer']}  (sandbox schema: {info['sandbox_schema']})")
