"""Equivalence harness: prove candidate == baseline. Cheapest checks first; RAISE on divergence.

Inputs may be a table name (str) or a DataFrame. The baseline is the reference truth.
"""
import math

_NUMERIC = ("int", "long", "short", "bigint", "double", "float", "decimal")
_FLOATY = ("double", "float", "decimal")


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
        same = (math.isclose(va, vb, rel_tol=1e-9, abs_tol=epsilon)
                if numeric and va is not None and vb is not None else va == vb)
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
    cols, nd = sorted(ca), _ndigits(epsilon)

    def norm(df):
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


def assert_equivalent(spark, baseline, candidate, *, epsilon: float = 1e-6,
                      partition_cols=None) -> dict:
    """Full ladder; RAISE with a localized reason on the first divergence. Never soften epsilon."""
    out = {"counts": check_counts(spark, baseline, candidate, partition_cols)}
    if not out["counts"]["passed"]:
        raise ValueError(f"Equivalence FAILED at counts: {out['counts']}")
    out["fingerprint"] = check_fingerprint(spark, baseline, candidate, epsilon)
    if not out["fingerprint"]["passed"]:
        raise ValueError(f"Equivalence FAILED at fingerprint: {out['fingerprint']['diffs']}")
    out["except_all"] = check_except_all(spark, baseline, candidate, epsilon)
    if not out["except_all"]["passed"]:
        raise ValueError(f"Equivalence FAILED at EXCEPT ALL: {out['except_all']}")
    out["passed"] = True
    return out
