# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Deploy factory assets
# MAGIC Provisions the Unity Catalog assets the Pipeline Optimization Factory needs:
# MAGIC schema, `opt_config`, `optimization_audit`, `get_opt_config`, and the governance views.
# MAGIC
# MAGIC Param-driven with defaults (edit the widgets, then Run All). Idempotent
# MAGIC (`CREATE ... IF NOT EXISTS` / `OR REPLACE`). Logic lives in `provision.py` so it is also
# MAGIC callable: `from provision import provision; provision(spark, factory_catalog=...)`.

# COMMAND ----------

from provision import DEFAULTS, provision

dbutils.widgets.text("factory_catalog", DEFAULTS["factory_catalog"], "Factory catalog")
dbutils.widgets.text("factory_schema", DEFAULTS["factory_schema"], "Factory schema")
dbutils.widgets.text("sandbox_schema", DEFAULTS["sandbox_schema"], "Sandbox schema")
dbutils.widgets.dropdown("create_catalogs", "true", ["true", "false"], "Create catalogs?")

# COMMAND ----------

info = provision(
    spark,
    factory_catalog=dbutils.widgets.get("factory_catalog"),
    factory_schema=dbutils.widgets.get("factory_schema"),
    sandbox_schema=dbutils.widgets.get("sandbox_schema"),
    create_catalogs=dbutils.widgets.get("create_catalogs") == "true",
)

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

fc, fs = info["factory"].split(".")
# information_schema is portable (SHOW VIEWS IN catalog.schema isn't supported on serverless).
display(spark.sql(
    f"SELECT table_name, table_type FROM {fc}.information_schema.tables "
    f"WHERE table_schema = '{fs}' ORDER BY table_type, table_name"))
print(f"Factory provisioned at {info['factory']}  (sandbox schema: {info['sandbox_schema']})")
