# Genie Code Pipeline Optimizer

Speed up a slow Databricks job without risking its results.

Point the optimizer at a job; it works through the job's notebooks, rewrites each one, and promotes the new version only after proving — on real data, in a sandbox — that it produces exactly the same result as the original. Every step is audited, including Genie's own explanation of why a change is safe, and production is never touched until you approve.

Built on the governed pattern of the Agentic Model Factory: skills, an append-only audit trail, hard gates that stop a bad change, and gated promotion. Here the guard is the equivalence gate.

## Quickstart

Invoke the orchestrator skill on demand, from the deployed `genie_code_optimizer` project folder so the skills and `lib/` imports resolve:

```
@optimize-pipeline <job_name>
```

`optimize-pipeline` is self-contained: it owns the flow, the two human-approval gates, the audit contract, and the guardrails, and it drives the sub-skills.
Nothing runs until you invoke it.

## How it works

You name a job. The optimizer ranks its notebooks by real cost and recommends the biggest hotspot; you confirm which to tackle. It then diagnoses that notebook, proposes the optimizations for your approval, generates a new version, and proves it on sandbox clones — never on production — before anything is promoted.

```
@optimize-pipeline <job_name>
   bootstrap the config from the job → rank the notebooks by cost → you select which to do
   │
   ├─ per selected notebook:  detect tables → profile → propose  (you approve)
   │                          generate v2 → sandbox clones → PROTOCOL + EQUIVALENCE + SECURITY gates
   │
   ├─ once all selected notebooks pass:  FLOW gate — run the whole optimized job on clones and
   │                                     compare every final table to the original run's real output
   ▼
   you approve → promote (repoint the job to v2; the original job is untouched)
```

Two gates decide correctness, and both are hard.
*Step equivalence* proves each notebook's v2 produces the same result as its original, on the full table.
*Flow equivalence* proves the whole optimized job reproduces the original job's output — a job that merely runs green is never accepted as proof.
Performance is measured and reported, but a change is promoted on correctness, not on speed alone.

Production is never touched during validation: every clone and write lands in a dedicated sandbox schema, and promotion only repoints the job definition.
Running the promoted job in production is a separate, human-gated deploy.

## Learn more

- **[Architecture](docs/architecture.md)** — the two loops, the gates in full, the two levels of equivalence, and the guardrails that hold the guarantee.
- **[Skills](docs/skills.md)** — each skill and what it does.
- **[Configuration](docs/configuration.md)** — settings, per-job parameters, the workspace layout, and compute.
- **[Deploy](docs/deploy.md)** — deploying to a workspace with DABs, incremental syncs, and the repo layout.

## Where it stands

The full flow — diagnose, approve, prove, promote — runs end to end on a live workspace. Both levels of equivalence, per-notebook and whole-job, are enforced on every promotion, and the whole-job harness is verified end to end.
