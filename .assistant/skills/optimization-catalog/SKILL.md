---
name: optimization-catalog
description: Reference catalog of the pipeline optimization techniques the factory can apply. NOT an @-entry point — consumed by perf-profile (symptom → technique) and optimize-notebook (technique → transformation recipe). Each technique has its own resource file with symptom, detection signal, before→after recipe, equivalence risk, and guard notes. Load ONLY the resource file for the chosen technique.
---

# optimization-catalog

Do NOT reimplement techniques inline. `perf-profile` maps a measured symptom to a technique
name here; `optimize-notebook` loads the matching `resources/<technique>.md` and applies its
recipe. Read one file, not all six.

## Symptom → technique

| Measured symptom (from perf-profile) | Technique | Resource file |
|---|---|---|
| Large shuffle on a join where one side is small (< ~30 MB) | Broadcast join | `resources/broadcast-join.md` |
| Task-time skew on a hot join/group key; AQE not enough | Skew (salting + AQE) | `resources/skew-salting-aqe.md` |
| Full scans / poor pruning; partition fragility; frequent filters on a few columns | Clustering | `resources/clustering-zorder-liquid.md` |
| Many small files; long list/scan time; high file count per read | Small files | `resources/optimize-small-files.md` |
| Python/Scala UDF in the hot path (defeats Photon) | de-UDF | `resources/de-udf.md` |
| Expensive aggregate/gold rebuilt in full every run | Incremental MV | `resources/incremental-mv.md` |

## Every resource file has the same shape

1. **Symptom** — what perf-profile sees.
2. **Detection signal** — the query/metric that confirms it (reuse `fe-workflows:performance-tuning/resources/diagnostic-queries.sql` where noted).
3. **Recipe** — before → after transformation.
4. **Equivalence risk** — whether the result can change, and what the `EXCEPT ALL` gate must catch.
5. **Guard notes** — what to check before promotion.

## Equivalence-risk tiers (drives how hard the gate runs)

- **Zero-risk rewrite** (broadcast join, small-files OPTIMIZE, clustering): result is provably
  identical by construction; gate is a fast confirmation.
- **Semantics-preserving rewrite** (de-UDF, salting): result *should* be identical but the rewrite
  touches logic — the `EXCEPT ALL` both-ways proof is mandatory and full (not sampled).
- **Refresh-semantics change** (incremental MV): output is the same steady-state table but the
  write path changes (full recompute → incremental) — gate must prove the incremental result equals
  the full recompute over the same pinned inputs.
