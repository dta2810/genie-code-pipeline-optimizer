# Copy to deploy/env.sh (gitignored), fill in, then `source deploy/env.sh` before deploy/sync.
# One config point for where everything lives.

# Workspace folder that houses ALL assets (skills, lib, deploy, sql, generated v2 notebooks).
export PO_WORKSPACE_HOME="/Workspace/Users/<you>/genie_code_optimizer"

# Unity Catalog optimizer schema (opt_config, optimization_audit, get_opt_config, governance views).
export PO_OPTIMIZER_CATALOG="main"
export PO_OPTIMIZER_SCHEMA="genie_optimizer"

# Sandbox SCHEMA for table clones + isolated runs. Created lazily inside each target's OWN
# catalog (no CREATE CATALOG needed). Clones are named <origschema>__<table> to avoid collision.
export PO_SANDBOX_SCHEMA="pipeline_opt_sandbox"

# Dedicated cluster for the HEAVY sandbox runs + wall-clock benchmark (control work stays serverless).
# Empty = fall back to the serverless session (fine at sample scale; large runs may time out).
export PO_COMPUTE_CLUSTER_ID=""

# Databricks CLI profile to target — an authenticated profile from `~/.databrickscfg`
# (create one with `databricks auth login`). This is a CLI profile, NOT a bundle target.
export DATABRICKS_CONFIG_PROFILE="DEFAULT"
