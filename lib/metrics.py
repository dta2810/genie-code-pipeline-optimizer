"""Portable, data-driven perf signals for ONE notebook's target tables — NO logfood, works in any
workspace. Feeds the perf-profile deep dive (which notebook is already selected) and the audit `perf`
map. Two always-available sources:

  * system.query.history  — spill, bytes/rows scanned, row amplification, time breakdown, for the
    statements that touch this notebook's tables. (True per-stage SHUFFLE bytes are NOT columns here;
    they live in the query profile. We surface spill + the indirect signals, and flag shuffle/skew as
    "confirm in the query profile" rather than fabricating a number.)
  * Delta DESCRIBE HISTORY operationMetrics — files/rows written vs removed + scan/rewrite time, i.e.
    the full-rewrite-vs-delta-touch I/O pattern. Always present for Delta targets.

Both are read-only. Match to a notebook is by its target-table names (from detect-tables).
"""

# system.query.history columns verified against the performance-tuning diagnostic queries.
_QH_COLS = ("statement_id, executed_by, start_time, execution_status, "
            "total_duration_ms, execution_duration_ms, compilation_duration_ms, "
            "result_fetch_duration_ms, read_rows, produced_rows, read_bytes, spilled_local_bytes")


def _short(table_fqn: str) -> str:
    return table_fqn.replace("`", "").split(".")[-1]


def query_history_signals(spark, target_tables, *, lookback_days: int = 7,
                          min_duration_ms: int = 0) -> dict:
    """Worst recent statement (by total duration) touching any of `target_tables`, plus aggregates.

    Portable to any workspace. Returns available=False (with a reason) if nothing matched — cluster
    Spark SQL is not always captured in system.query.history, so the caller falls back to the Delta
    signal. Never raises.
    """
    names = [_short(t) for t in (target_tables or [])]
    if not names:
        return {"available": False, "reason": "no target tables"}
    like = " OR ".join([f"lower(statement_text) LIKE '%{n.lower()}%'" for n in names])
    try:
        rows = spark.sql(f"""
            SELECT {_QH_COLS}
            FROM system.query.history
            WHERE start_time >= current_date() - INTERVAL {int(lookback_days)} DAYS
              AND execution_status = 'FINISHED'
              AND total_duration_ms >= {int(min_duration_ms)}
              AND ({like})
            ORDER BY total_duration_ms DESC
            LIMIT 50
        """).collect()
    except Exception as e:
        return {"available": False, "reason": f"query.history unavailable ({str(e)[:120]})"}
    if not rows:
        return {"available": False, "reason": "no matching statements in window "
                "(cluster Spark SQL may not be captured in system.query.history)"}

    w = rows[0].asDict()  # the worst statement
    read_b = w.get("read_bytes") or 0
    spill_b = w.get("spilled_local_bytes") or 0
    read_r = w.get("read_rows") or 0
    prod_r = w.get("produced_rows") or 0
    total = w.get("total_duration_ms") or 0
    return {
        "available": True,
        "matched_statements": len(rows),
        "statement_id": w.get("statement_id"),
        "total_duration_ms": total,
        "execution_duration_ms": w.get("execution_duration_ms"),
        "compilation_duration_ms": w.get("compilation_duration_ms"),
        "read_bytes": read_b,
        "read_rows": read_r,
        "produced_rows": prod_r,
        "spilled_local_bytes": spill_b,
        "spill_pct": round(spill_b * 100.0 / read_b, 1) if read_b else 0.0,
        "row_amplification": round(prod_r / read_r, 2) if read_r else None,
    }


def delta_write_signals(spark, target_table: str, *, lookback_writes: int = 5) -> dict:
    """The latest write op's operationMetrics for a Delta target (files/rows added vs removed,
    scan/rewrite time) — the full-rewrite-vs-delta-touch signal. Portable; never raises.
    """
    try:
        hist = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT {int(lookback_writes)}").collect()
    except Exception as e:
        return {"available": False, "reason": f"no Delta history ({str(e)[:120]})"}
    writes = [r for r in hist
              if (r["operation"] or "").upper() in
              ("WRITE", "MERGE", "DELETE", "UPDATE", "CREATE OR REPLACE TABLE AS SELECT",
               "CREATE TABLE AS SELECT", "TRUNCATE", "REPLACE TABLE AS SELECT")]
    if not writes:
        return {"available": False, "reason": "no write op in recent history"}
    op = writes[0]
    m = {k: v for k, v in (op["operationMetrics"] or {}).items()}

    def _i(*keys):
        for k in keys:
            if k in m and m[k] not in (None, ""):
                try:
                    return int(m[k])
                except ValueError:
                    pass
        return 0

    added = _i("numTargetFilesAdded", "numFiles", "numAddedFiles")
    removed = _i("numTargetFilesRemoved", "numRemovedFiles", "numDeletedFiles")
    out_rows = _i("numOutputRows", "numTargetRowsInserted")
    return {
        "available": True,
        "operation": op["operation"],
        "files_added": added,
        "files_removed": removed,
        "output_rows": out_rows,
        # rewrite ratio ~1 => the op rewrote roughly as much as it removed = full-rewrite pattern
        "rewrite_ratio": round(removed / added, 2) if added else None,
        "scan_time_ms": _i("scanTimeMs"),
        "rewrite_time_ms": _i("rewriteTimeMs"),
        "execution_time_ms": _i("executionTimeMs"),
    }


# Portable signal -> catalog technique(s). Shuffle/skew are only *suspected* from system tables.
def diagnose(qh: dict, dw_list: list[dict]) -> dict:
    """Turn the portable signals into '4 S's' flags + suggested techniques + a short insight."""
    flags, techniques = [], []
    qh = qh or {}
    dw_list = [d for d in (dw_list or []) if d.get("available")]

    if qh.get("available"):
        if (qh.get("spilled_local_bytes") or 0) > 0:
            flags.append(f"SPILL: {qh['spilled_local_bytes']:,} bytes ({qh['spill_pct']}% of read)")
            techniques += ["skew-salting/AQE", "broadcast-join (if a small side)"]
        amp = qh.get("row_amplification")
        if amp and amp >= 3:
            flags.append(f"ROW AMPLIFICATION: produced/read = {amp}x (exploding join?)")
            techniques.append("review join keys / broadcast")
        # shuffle is not a query.history column — flag only as a hypothesis to confirm downstream
        if (qh.get("spilled_local_bytes") or 0) > 0 or (amp and amp >= 3):
            flags.append("SHUFFLE/SKEW suspected — confirm in the query profile (not in system tables)")

    for dw in dw_list:
        rr = dw.get("rewrite_ratio")
        if rr is not None and rr >= 0.8 and dw.get("files_removed", 0) > 0:
            flags.append(f"FULL-REWRITE pattern on {dw['operation']} "
                         f"(removed {dw['files_removed']} / added {dw['files_added']} files)")
            techniques += ["MERGE / dynamic-partition INSERT OVERWRITE instead of full rewrite"]

    if not flags:
        flags.append("no strong portable signal — inspect the query profile for shuffle/skew detail")
    # de-dup, keep order
    techniques = list(dict.fromkeys(techniques))
    insight = "; ".join(flags[:4])
    return {"flags": flags, "suggested_techniques": techniques, "insight": insight}


def profile_notebook(spark, *, target_tables, lookback_days: int = 7) -> dict:
    """Full portable perf profile for one notebook's targets: query.history + Delta metrics +
    a '4 S's' diagnosis. Returns a dict ready to log into the audit `perf` map.
    """
    qh = query_history_signals(spark, target_tables, lookback_days=lookback_days)
    dw = {t: delta_write_signals(spark, t) for t in (target_tables or [])}
    diag = diagnose(qh, list(dw.values()))
    return {"query_history": qh, "delta_writes": dw, "diagnosis": diag}


def perf_map(profile: dict) -> dict:
    """Flatten a profile into the STRING->STRING `perf` map for audit_log(perf=...)."""
    qh = profile.get("query_history", {})
    diag = profile.get("diagnosis", {})
    out = {
        "spilled_local_bytes": qh.get("spilled_local_bytes"),
        "spill_pct": qh.get("spill_pct"),
        "read_bytes": qh.get("read_bytes"),
        "row_amplification": qh.get("row_amplification"),
        "total_duration_ms": qh.get("total_duration_ms"),
        "shuffle": "not-in-system-tables (see query profile)",
        "flags": " | ".join(diag.get("flags", [])),
    }
    return {k: v for k, v in out.items() if v is not None}
