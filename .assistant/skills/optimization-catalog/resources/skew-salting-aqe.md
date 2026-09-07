# Skew (salting + AQE)

## Symptom
A few tasks in a join/aggregation stage run far longer than the rest — a hot key concentrates rows
on one partition. AQE skew handling alone does not clear it.

## Detection signal
- Spark stage: max task time >> median task time; one/few partitions with most of the rows.
- Known hot key (e.g. a dominant customer/route/null).
- diagnostic-queries.sql #3 (high spill) often accompanies extreme skew.

## Recipe
First rely on AQE:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

If AQE is insufficient, salt the hot key (spread it across N partitions):

```python
from pyspark.sql.functions import concat, lit, col, rand, floor, explode, sequence

num_salts = 10
large_df = (large_df
    .withColumn("salt", floor(rand() * num_salts).cast("int"))
    .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt"))))
small_df = (small_df
    .withColumn("salt", explode(sequence(lit(0), lit(num_salts - 1))))
    .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt"))))
result = large_df.join(small_df, "salted_key")
```

Drop the helper columns (`salt`, `salted_key`) before writing so the output schema matches v1.

## Equivalence risk
**Semantics-preserving rewrite.** Salting changes the physical key but must yield the same join
result. The `explode(sequence(...))` on the small side must cover exactly `0..num_salts-1`, and
helper columns must be projected away. Full both-ways `EXCEPT ALL` is mandatory — do not sample.

## Guard notes
- Verify the final projection equals v1's columns exactly (no leaked `salt`/`salted_key`).
- For aggregations, salting needs a two-stage aggregate (salted partial → de-salted final); confirm
  the de-salting sums/re-aggregates correctly or the counts will diverge.
- Non-determinism: `rand()` is fine here (it only distributes rows) but the sandbox run must pin
  inputs so the compare is stable.
