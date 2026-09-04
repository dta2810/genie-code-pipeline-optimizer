"""optimize-notebook: name + copy the original notebook to the v2 path. Genie edits the copy."""
import os
import sys
from datetime import datetime

if "__file__" in globals():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 4)))


def v2_path(original_path: str, optimized_folder: str) -> str:
    """<optimized_folder>/<notebook_name>_genie_opt_<timestamp>."""
    name = original_path.rstrip("/").split("/")[-1]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{optimized_folder.rstrip('/')}/{name}_genie_opt_{ts}"


def copy_notebook(original_path: str, dest_path: str) -> str:
    """Export the original notebook and import it at dest (starting point for the v2). Never edits source."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat
    w = WorkspaceClient()
    exported = w.workspace.export(original_path, format=ImportFormat.SOURCE)
    parent = dest_path.rsplit("/", 1)[0]
    w.workspace.mkdirs(parent)
    w.workspace.import_(dest_path, format=ImportFormat.SOURCE, content=exported.content, overwrite=True)
    return dest_path
