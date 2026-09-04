---
name: security-review
description: Late-stage security gate before promoting an optimized notebook. Reviews the v2 candidate (and the sandbox setup) for secrets, injection, access/PII broadening, writes outside the sandbox, unsafe UDF/external calls, and cost blowups. Runs after equivalence + perf pass, before the human promotion gate. Audited with an insight.
---

# security-review

Run as the **late gate** — after `equivalence-check` and `perf-benchmark` pass, before the human
promotion gate. Review the diff between v1 and the v2 candidate (plus how it runs), not just the
happy path. Findings block promotion until resolved.

## Checklist
- **Secrets / credentials** — no hardcoded tokens, keys, passwords, connection strings; use secret
  scopes / provided auth only.
- **Injection** — parameters (`{{job.parameters.*}}`, widgets, f-strings) are not concatenated into
  SQL/DDL in a way that allows injection; identifiers validated.
- **Access / grants** — the v2 does not broaden permissions (new `GRANT`, ownership change, wider
  scope) vs v1; runs with caller/OBO perms so masks & row-filters still apply.
- **PII exposure** — no new unmasking, no PII written to a wider-access location, no PII in logs.
- **Sandbox containment** — during test, every write targets the sandbox; no write to a production
  path (re-confirm the `sandbox-setup` remap/param-override was applied).
- **Unsafe execution** — no arbitrary external calls, shell-outs, or dynamic code from untrusted
  input introduced by the optimization; UDF/native rewrites don't call out unexpectedly.
- **Cost** — the optimization doesn't trade correctness/perf for a hidden cost blowup (e.g. cross
  join, exploding cache, unbounded broadcast).

## Finish
`audit_log(step="security_review", status="succeeded"|"failed", insight=<findings or "clean">)`.
On any finding: status `failed`, write the finding as the insight, and STOP — do not promote.
Aligns with the standing rule: a security review is the final gate before anything is shared.
