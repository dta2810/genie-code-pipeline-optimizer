# Copy to deploy/env.sh (gitignored), fill in, then `source deploy/env.sh` before deploy/sync.
# One config point for where everything lives.

# Workspace folder that houses ALL assets (skills, lib, deploy, sql, generated v2 notebooks).
export PO_WORKSPACE_HOME="/Workspace/Users/<you>/genie_code_optimizer"

# Unity Catalog factory schema (opt_config, optimization_audit, get_opt_config, governance views).
export PO_FACTORY_CATALOG="main"
export PO_FACTORY_SCHEMA="pipeline_opt_factory"

# Sandbox SCHEMA for table clones + isolated runs. Created lazily inside each target's OWN
# catalog (no CREATE CATALOG needed). Clones are named <origschema>__<table> to avoid collision.
export PO_SANDBOX_SCHEMA="pipeline_opt_sandbox"
# For the LATAM Flights_Pipeline_Final job (tables in main_david_thomas), use:
# export PO_SANDBOX_SCHEMA="genie_optimizer_v1_sandbox"

# Databricks CLI profile to target.
export DATABRICKS_CONFIG_PROFILE="DEFAULT"
