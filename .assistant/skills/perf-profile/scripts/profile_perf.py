"""perf-profile: portable, data-driven perf signals for the SELECTED notebook — no logfood.

Pulls spill / bytes / row-amplification from system.query.history and the full-rewrite-vs-delta-touch
signal from Delta operationMetrics, diagnoses the '4 S's', and records it in the audit `perf` map.
"""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 4)))

from lib import metrics  # noqa: E402
from lib.audit import audit_log  # noqa: E402


def top_statements(spark, hours: int = 72, limit: int = 20):
    """Most expensive recent statements from system.query.history (Genie correlates to the notebook)."""
    return spark.sql(f"""
        SELECT statement_id, executed_by, total_duration_ms,
               read_bytes, read_rows, produced_rows, statement_text
        FROM system.query.history
        WHERE end_time >= current_timestamp() - INTERVAL {int(hours)} HOURS
        ORDER BY total_duration_ms DESC
        LIMIT {int(limit)}
    """)


def profile(spark, *, job, notebook, target_tables, lookback_days: int = 7, notebook_path=None):
    """Portable perf profile for the selected notebook + audit it. Returns the profile dict so Genie
    can present the flags/signals and pick techniques at GATE 1.

    `target_tables` come from detect-tables. Signals: spill, bytes/rows scanned, row amplification
    (system.query.history) + files/rows rewritten & scan/rewrite time (Delta DESCRIBE HISTORY).
    Shuffle bytes are NOT in system tables — reported as 'confirm in the query profile', not faked.
    """
    prof = metrics.profile_notebook(spark, target_tables=target_tables, lookback_days=lookback_days)
    audit_log(spark, job=job, notebook=notebook, step="perf_profile", status="succeeded",
              perf=metrics.perf_map(prof), notebook_path=notebook_path,
              insight=prof["diagnosis"]["insight"])
    return prof


def record_profile(spark, *, job, notebook, insight: str):
    """Audit a bottleneck finding Genie derived manually (fallback when profile() isn't used)."""
    audit_log(spark, job=job, notebook=notebook, step="perf_profile",
              status="succeeded", insight=insight)
