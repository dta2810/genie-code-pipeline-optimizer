# de-UDF

Rewrite a Python/Scala UDF in the hot path as native Spark SQL / built-in functions. **Highest
equivalence risk in the catalog** — it changes logic, not just physical execution. Build-new; no
prior-art recipe to copy, only the pattern below.

## Symptom
A row-wise Python/Scala UDF on a large stage. Defeats Photon and forces per-row serialization.

## Detection signal
- UDF registration/usage in the notebook source (detect-tables/perf-profile flags it).
- Stage runs without Photon; high serialization/CPU time on a simple transform.
- Anti-patterns checklist: "Python/Scala UDFs → use built-in SQL functions".

## Recipe
Map the UDF body to built-in functions.

```python
# before: Python UDF (no Photon)
@udf(StringType())
def my_upper(s):
    return s.upper() if s else None
df = df.withColumn("name", my_upper(col("name")))

# after: built-in (Photon-optimized)
from pyspark.sql.functions import upper
df = df.withColumn("name", upper(col("name")))
```

For branching logic use `when/otherwise`, `coalesce`, `regexp_*`, `try_*`, `date_*`, etc. When a
UDF cannot be fully expressed natively, prefer a **pandas UDF (Arrow)** over a plain Python UDF as a
partial win, and flag the residual for human review.

## Equivalence risk
**Semantics-preserving rewrite — treat as high risk.** The native expression must reproduce the
UDF's behavior on every edge:
- **Nulls**: UDF null-handling vs built-in null-propagation often differ — replicate the
  `if s else None` branch explicitly.
- **Type coercion / rounding**: native casts may round or overflow differently than Python.
- **Errors**: a UDF that swallowed exceptions vs a built-in that returns NULL / raises.
- **Empty string vs null**, trimming, locale (`upper`/`lower` on non-ASCII).

Full both-ways `EXCEPT ALL` is mandatory and must be **full-scan, never sampled**. Add targeted
null / empty / boundary rows to the sandbox inputs so the gate actually exercises the edges.

## Guard notes
- If the UDF read external state (files, network, secrets) it is **not** a pure transform — do NOT
  de-UDF it silently; flag to the human (this is also a security-review concern).
- Non-determinism: a UDF using randomness/time is not equivalence-testable — abort and flag.
- Record in the audit `insight` exactly which edge cases the native version was checked against.
