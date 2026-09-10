# Skills

Each skill is a `SKILL.md` (Agent-Skills frontmatter) that describes *when and why*, and imports the matching `lib/` module through its `scripts/` for the *how*.
Behavior stays consistent and testable, never reimplemented inline.

| Skill | Role | What's in it |
|---|---|---|
| `optimize-pipeline` | **entry point / orchestrator** | `@optimize-pipeline` — the self-contained flow: bootstrap → select → per-notebook gates → whole-job flow-validate → promote. Owns the audit contract, the two human-approval gates, and the guardrails. |
| `detect-tables` | source/target discovery | Genie reads the notebook (no SQL parser), resolves dynamic and widget table names, cross-checks Unity Catalog lineage, and flags non-determinism (`current_timestamp`, random, unordered aggregation). Human-confirmed. |
| `perf-profile` | bottleneck diagnosis | Root-causes the chosen notebook from the job JSON, `system.query.history`, and Delta `DESCRIBE HISTORY` (full-rewrite-vs-delta-touch, spill, row amplification). Proposes the full applicable technique stack up front. |
| `optimization-catalog` | technique recipes | Six recipes (broadcast, skew, clustering → Z-ORDER, small files, de-UDF, incremental MV), each with symptom → detection → before/after → equivalence-risk tier → guard. A starting set, not a cage. |
| `optimize-notebook` | generate v2 | Writes `<ntb>_genie_opt_<ts>` (never edits the original). Enforces no protocol bump and Spark-Connect-safe rewrites — a partition reload uses `INSERT … REPLACE WHERE <keys>` with literal keys, never a bare `INSERT OVERWRITE` (a static full-table overwrite that drops other partitions). |
| `sandbox-setup` | isolation | Shallow-clones targets with their data, pins inputs by time-travel, remaps every write to the sandbox schema, and verifies no production reference remains. Sampled-vs-full tier per operation. |
| `equivalence-check` | **step gate** (hard) | Per notebook: counts → column fingerprint → `EXCEPT ALL` both ways, on the full table, epsilon by risk tier. Raises on divergence. |
| `flow-validate` | **flow gate** (hard) | Whole job: replays the optimized job on clones from a real past run's inputs and compares each final table to that run's recorded output (or a two-schema A/B). Catches composition and scale bugs a step gate cannot. |
| `perf-benchmark` | performance reading (advisory) | Median of N on the dedicated cluster. Separates the equivalence proof from the performance signal; on serverless it reports structural I/O rather than a contaminated wall-clock. |
| `security-review` | **late security gate** (hard) | Reviews the v2 for secrets, injection, access or PII broadening, out-of-sandbox writes, unsafe calls, and cost blowups. Any finding blocks promotion. |

## The optimization catalog is a starting set, not a cage

`optimization-catalog` ships the six reference recipes above.
The optimizer may also use Genie Code's own skills — `writing-sql`, `table-optimization`, `data-modification`, `performance-tuning` — when they offer a better technique.
It names the source at the first approval gate and in the audit.
Safety comes from the gates, not from limiting where a technique comes from.
The one hard prohibition, regardless of source, is a protocol or table-feature bump.
