"""Equivalence harness: prove candidate == baseline. Cheapest checks first; RAISE on divergence.

Inputs may be a table name (str) or a DataFrame. The baseline is the reference truth.
"""
import math

_NUMERIC = ("int", "long", "short", "bigint", "double", "float", "decimal")
_FLOATY = ("double", "float", "decimal")

# Risk tier (from optimization-catalog) drives the epsilon policy. A zero-risk rewrite
# (broadcast/clustering/small-files) is identical by construction -> demand EXACT floats;
# any float drift signals a bug, not reordering. Semantics-preserving / refresh-change
# rewrites (de-UDF, salting, incremental-MV) can reassociate floats -> allow epsilon slack.
_EPSILON_BY_TIER = {"zero_risk": 0.0, "semantics_preserving": 1e-6, "refresh_change": 1e-6}


def _df(spark, x):
    return spark.table(x) if isinstance(x, str) else x


def _ndigits(epsilon: float) -> int:
    return max(0, int(round(-math.log10(epsilon)))) if epsilon and epsilon > 0 else 12


def check_counts(spark, baseline, candidate, partition_cols=None) -> dict:
    """Total (and optional per-partition) row counts must match."""
    a, b = _df(spark, baseline), _df(spark, candidate)
    res = {"baseline_count": a.count(), "candidate_count": b.count()}
    res["passed"] = res["baseline_count"] == res["candidate_count"]
    if partition_cols:
        from pyspark.sql import functions as F
        ga = a.groupBy(*partition_cols).count().withColumnRenamed("count", "_cb")
        gb = b.groupBy(*partition_cols).count().withColumnRenamed("count", "_cc")
        mism = (ga.join(gb, list(partition_cols), "full_outer")
                  .where("coalesce(_cb,-1) <> coalesce(_cc,-1)").count())
        res["partition_mismatches"] = mism
        res["passed"] = res["passed"] and mism == 0
    return res


def check_fingerprint(spark, baseline, candidate, epsilon: float = 1e-6) -> dict:
    """Per-column null-count / distinct / sum-min-max must match (numerics within epsilon)."""
    from pyspark.sql import functions as F
    a, b = _df(spark, baseline), _df(spark, candidate)
    cols = sorted(set(a.columns) & set(b.columns))

    def fp(df):
        dt = dict(df.dtypes)
        aggs = []
        for c in cols:
            aggs.append(F.count(F.when(F.col(c).isNull(), 1)).alias(f"{c}__nulls"))
            aggs.append(F.approx_count_distinct(c).alias(f"{c}__ndv"))
            if any(t in dt[c] for t in _NUMERIC):
                aggs += [F.sum(c).cast("double").alias(f"{c}__sum"),
                         F.min(c).cast("double").alias(f"{c}__min"),
                         F.max(c).cast("double").alias(f"{c}__max")]
        return df.agg(*aggs).collect()[0].asDict()

    fa, fb = fp(a), fp(b)
    diffs = {}
    for k in fa:
        va, vb = fa.get(k), fb.get(k)
        numeric = k.endswith(("__sum", "__min", "__max"))
        if numeric and va is not None and vb is not None:
            same = (va == vb) if epsilon == 0 else math.isclose(va, vb, rel_tol=1e-9, abs_tol=epsilon)
        else:
            same = va == vb
        if not same:
            diffs[k] = (va, vb)
    return {"passed": not diffs, "diffs": diffs, "columns": cols}


def check_except_all(spark, baseline, candidate, epsilon: float = 1e-6) -> dict:
    """Hard proof: A EXCEPT ALL B and B EXCEPT ALL A both empty (order/dup-safe, floats rounded)."""
    from pyspark.sql import functions as F
    a, b = _df(spark, baseline), _df(spark, candidate)
    ca, cb = set(a.columns), set(b.columns)
    if ca != cb:
        return {"passed": False, "reason": "schema mismatch",
                "only_baseline": sorted(ca - cb), "only_candidate": sorted(cb - ca)}
    cols, exact = sorted(ca), (epsilon == 0)
    nd = _ndigits(epsilon)

    def norm(df):
        if exact:  # zero-risk tier: compare floats bit-for-bit, no rounding
            return df.select(*cols)
        dt = dict(df.dtypes)
        out = df
        for c in cols:
            if any(t in dt[c] for t in _FLOATY):
                out = out.withColumn(c, F.round(F.col(c), nd))
        return out.select(*cols)

    na, nb = norm(a), norm(b)
    try:
        extra_base = na.exceptAll(nb).count()   # rows in baseline missing from candidate
        extra_cand = nb.exceptAll(na).count()   # rows in candidate not in baseline
    except Exception as e:  # type mismatch on a shared column, etc.
        return {"passed": False, "reason": f"exceptAll failed (type mismatch?): {e}"}
    return {"passed": extra_base == 0 and extra_cand == 0,
            "in_baseline_not_candidate": extra_base, "in_candidate_not_baseline": extra_cand}


def _protocol(spark, table) -> tuple[int, int, set]:
    """(minReaderVersion, minWriterVersion, table-features) of a Delta table via DESCRIBE DETAIL."""
    r = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0].asDict()
    return (int(r.get("minReaderVersion") or 1), int(r.get("minWriterVersion") or 1),
            set(r.get("tableFeatures") or []))


def assert_no_protocol_change(spark, baseline, candidate) -> dict:
    """HARD gate: the candidate output must NOT raise the Delta reader/writer protocol or add a
    table feature vs the baseline. Protocol-bumping optimizations (liquid clustering `CLUSTER BY`,
    deletion vectors, row tracking, generated columns, v2 checkpoint) change the table's downstream
    read/write CONTRACT and can break consumers — they are forbidden, never proposed, never applied.
    Run in the sandbox before promotion. RAISES on any bump.
    """
    br, bw, bf = _protocol(spark, baseline)
    cr, cw, cf = _protocol(spark, candidate)
    added = sorted(cf - bf)
    if cr > br or cw > bw or added:
        raise ValueError(
            f"Protocol change BLOCKED: candidate needs reader/writer ({cr}/{cw}) vs baseline "
            f"({br}/{bw}); added table features {added}. Protocol/feature bumps are forbidden — "
            "they change the downstream contract. Use non-bumping equivalents (Z-ORDER not liquid "
            "clustering) and, for CREATE OR REPLACE, preserve the source table's TBLPROPERTIES/"
            "protocol instead of accepting newer engine defaults.")
    return {"protocol_ok": True, "reader": cr, "writer": cw, "features": sorted(cf)}


def assert_equivalent(spark, baseline, candidate, *, risk_tier: str = "semantics_preserving",
                      epsilon: float | None = None, partition_cols=None,
                      baseline_is_full_recompute: bool = False) -> dict:
    """Full ladder; RAISE with a localized reason on the first divergence. Never soften epsilon.

    `risk_tier` (from optimization-catalog) sets the epsilon policy when `epsilon` is not given
    explicitly: zero_risk -> exact (0.0), semantics_preserving / refresh_change -> 1e-6. The full
    `EXCEPT ALL` always runs (fingerprint is a pre-filter, never proof). For `refresh_change` the
    baseline MUST be the full recompute over the same pinned inputs (pass
    `baseline_is_full_recompute=True`) or we refuse to certify.
    """
    eps = epsilon if epsilon is not None else _EPSILON_BY_TIER.get(risk_tier, 1e-6)
    if risk_tier == "refresh_change" and not baseline_is_full_recompute:
        raise ValueError("refresh_change tier requires baseline_is_full_recompute=True "
                         "(compare the incremental result against a full recompute, not v1's output).")
    meta = {"risk_tier": risk_tier, "epsilon": eps,
            "baseline_is_full_recompute": baseline_is_full_recompute}

    out = {"counts": check_counts(spark, baseline, candidate, partition_cols), **meta}
    if not out["counts"]["passed"]:
        raise ValueError(f"Equivalence FAILED at counts [{risk_tier}]: {out['counts']}")
    out["fingerprint"] = check_fingerprint(spark, baseline, candidate, eps)
    if not out["fingerprint"]["passed"]:
        raise ValueError(f"Equivalence FAILED at fingerprint [{risk_tier}]: {out['fingerprint']['diffs']}")
    out["except_all"] = check_except_all(spark, baseline, candidate, eps)
    if not out["except_all"]["passed"]:
        raise ValueError(f"Equivalence FAILED at EXCEPT ALL [{risk_tier}]: {out['except_all']}")
    out["passed"] = True
    return out
