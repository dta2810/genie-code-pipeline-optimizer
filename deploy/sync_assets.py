#!/usr/bin/env python3
"""Cross-platform asset sync (Windows / macOS / Linux) — the no-bash equivalent of sync_assets.sh.

Imports the Genie Code assets (.assistant, lib, deploy, sql) to PO_WORKSPACE_HOME with
`databricks workspace import-dir --overwrite`: it adds/overwrites the copied dirs and NEVER prunes,
so it is safe over runtime artifacts (the per-job jobs/<job>/optimized|validation|driver notebooks).

Requires the Databricks CLI on PATH and an authenticated profile.
Usage: set PO_WORKSPACE_HOME (and optionally DATABRICKS_CONFIG_PROFILE), then `python deploy/sync_assets.py`.
"""
import os
import subprocess
import sys
from pathlib import Path

home = os.environ.get("PO_WORKSPACE_HOME")
if not home:
    sys.exit("Set PO_WORKSPACE_HOME first (see deploy/env.example.sh).")
profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
repo = Path(__file__).resolve().parent.parent

print(f"Syncing {repo} -> {home}  (profile: {profile})")
for d in (".assistant", "lib", "deploy", "sql", "config"):
    print(f"  - {d}/")
    subprocess.run(
        ["databricks", "workspace", "import-dir", str(repo / d), f"{home}/{d}",
         "--overwrite", "--profile", profile],
        check=True,
    )
print(f"Done. Assets live at {home}")
