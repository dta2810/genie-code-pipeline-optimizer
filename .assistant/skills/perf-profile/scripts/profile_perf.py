"""perf-profile: pull expensive statements from system tables for Genie Code to attribute."""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 4)))

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


def record_profile(spark, *, job, notebook, insight: str):
    """Audit the bottleneck finding Genie derived from the profile + query plan."""
    audit_log(spark, job=job, notebook=notebook, step="perf_profile",
              status="succeeded", insight=insight)
