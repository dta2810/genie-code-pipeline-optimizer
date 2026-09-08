# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Deploy optimizer assets
# MAGIC Provisions the Unity Catalog assets the Genie Code Pipeline Optimizer needs:
# MAGIC schema, `opt_config`, `optimization_audit`, `get_opt_config`, and the governance views.
# MAGIC
# MAGIC Param-driven with defaults (edit the widgets, then Run All). Idempotent
# MAGIC (`CREATE ... IF NOT EXISTS` / `OR REPLACE`). Logic lives in `provision.py` so it is also
# MAGIC callable: `from provision import provision; provision(spark, optimizer_catalog=...)`.

# COMMAND ----------

from provision import DEFAULTS, provision

dbutils.widgets.text("optimizer_catalog", DEFAULTS["optimizer_catalog"], "Optimizer catalog")
dbutils.widgets.text("optimizer_schema", DEFAULTS["optimizer_schema"], "Optimizer schema")
dbutils.widgets.text("sandbox_schema", DEFAULTS["sandbox_schema"], "Sandbox schema")
dbutils.widgets.text("compute_cluster_id", DEFAULTS["compute_cluster_id"], "Dedicated cluster id (heavy runs)")
dbutils.widgets.dropdown("create_catalogs", "true", ["true", "false"], "Create catalogs?")

# COMMAND ----------

info = provision(
    spark,
    optimizer_catalog=dbutils.widgets.get("optimizer_catalog"),
    optimizer_schema=dbutils.widgets.get("optimizer_schema"),
    sandbox_schema=dbutils.widgets.get("sandbox_schema"),
    compute_cluster_id=dbutils.widgets.get("compute_cluster_id"),
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
