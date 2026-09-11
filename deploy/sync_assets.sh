#!/usr/bin/env bash
# Sync the Genie Code assets (skills, instructions, lib, deploy, sql) to the workspace home.
# All assets live under one configurable folder: $PO_WORKSPACE_HOME. Structure is preserved so
# script imports (sys.path -> repo root) resolve, and Genie Code auto-loads .assistant/skills.
#
# Usage:  source deploy/env.sh && bash deploy/sync_assets.sh
set -euo pipefail

HOME_PATH="${PO_WORKSPACE_HOME:?set PO_WORKSPACE_HOME (see deploy/env.example.sh)}"
PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"

echo "Syncing $REPO -> $HOME_PATH  (profile: $PROFILE)"
for d in .assistant lib deploy sql config; do
  echo "  · $d/"
  databricks workspace import-dir "$REPO/$d" "$HOME_PATH/$d" --overwrite --profile "$PROFILE"
done
echo "Done. Assets live at $HOME_PATH"
