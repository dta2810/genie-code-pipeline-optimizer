# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Deploy factory assets
# MAGIC Provisions the Unity Catalog assets the Pipeline Optimization Factory needs:
# MAGIC schema, `opt_config`, `optimization_audit`, `get_opt_config`, and the governance views.
# MAGIC
# MAGIC Idempotent (CREATE ... IF NOT EXISTS / OR REPLACE). Set the widgets, then Run All.
# MAGIC The sandbox catalog holds table clones + remapped writes; per-schema clones are created
# MAGIC on demand by `sandbox-setup`, so this notebook only ensures the catalog exists.

# COMMAND ----------

dbutils.widgets.text("factory_catalog", "main", "Factory catalog")
dbutils.widgets.text("factory_schema", "pipeline_opt_factory", "Factory schema")
dbutils.widgets.text("sandbox_catalog", "opt_sandbox", "Sandbox catalog")

FACTORY_CATALOG = dbutils.widgets.get("factory_catalog")
FACTORY_SCHEMA = dbutils.widgets.get("factory_schema")
SANDBOX_CATALOG = dbutils.widgets.get("sandbox_catalog")

# COMMAND ----------

import os

# repo root = parent of this notebook's folder (deploy/)
REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
SQL_DIR = os.path.join(REPO, "sql")


def run_sql_file(fname):
    """Read a sql file, substitute {{catalog}}/{{schema}}, execute each ';'-separated statement."""
    with open(os.path.join(SQL_DIR, fname)) as f:
        body = f.read().replace("{{catalog}}", FACTORY_CATALOG).replace("{{schema}}", FACTORY_SCHEMA)
    for stmt in [s.strip() for s in body.split(";") if s.strip()]:
        spark.sql(stmt)
    print(f"✓ {fname}")

# COMMAND ----------

# MAGIC %md ## Catalogs + schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {SANDBOX_CATALOG}")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {FACTORY_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FACTORY_CATALOG}.{FACTORY_SCHEMA}")
print(f"✓ sandbox={SANDBOX_CATALOG}  factory={FACTORY_CATALOG}.{FACTORY_SCHEMA}")

# COMMAND ----------

# MAGIC %md ## Tables → config function → governance views

# COMMAND ----------

run_sql_file("tables.sql")            # opt_config + optimization_audit
run_sql_file("config_function.sql")   # get_opt_config
run_sql_file("governance_views.sql")  # v_run_activity / v_guard_events / v_optimization_scorecard

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {FACTORY_CATALOG}.{FACTORY_SCHEMA}"))
display(spark.sql(f"SHOW VIEWS IN {FACTORY_CATALOG}.{FACTORY_SCHEMA}"))
print("Set env vars for the harness: "
      f"PO_FACTORY_CATALOG={FACTORY_CATALOG}  PO_FACTORY_SCHEMA={FACTORY_SCHEMA}  "
      f"PO_SANDBOX_CATALOG={SANDBOX_CATALOG}")
