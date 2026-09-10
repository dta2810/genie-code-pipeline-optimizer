"""flow.py — job-LEVEL (flow) equivalence.

Per-notebook equivalence (equivalence.py) proves each STEP v1==v2 on independently pinned/cloned
inputs. It does NOT prove the WHOLE original job's final tables equal the WHOLE optimized job's
final tables — the DAG composes stages, and each step was validated on separate clones.
flow-validate closes that gap:

  clone every table (sources + targets) into TWO schemas from ONE snapshot (_orig / _opt, identical)
  -> run the ORIGINAL job DAG into _orig and the OPTIMIZED job DAG into _opt from identical inputs
     (widget-param schema swap, on dedicated compute)
  -> compare every final target (counts -> fingerprint -> EXCEPT ALL)
  -> audit a flow_equivalence verdict.

Prod-safe: the jobs are re-pointed to the sandbox schemas via base_parameters; prod tables are
never written. Sampled start (SAME seed both sides) keeps a huge driver cheap; partition-scoped
targets are cloned full (SHALLOW CLONE) so partitioning survives.
"""
import time

from databricks.sdk import WorkspaceClient

from . import equivalence
from .audit import audit_log


def flow_schema_names(base_schema: str, *, tag: str = "flow") -> tuple[str, str]:
    """The two parallel sandbox schema names (same catalog as the job's tables)."""
    return f"{base_schema}_{tag}_orig", f"{base_schema}_{tag}_opt"


def _resolve_version(spark, fqn: str, when) -> int:
    """Resolve a pin to a concrete Delta version. An int is returned as-is (explicit version). A
    timestamp string resolves to the latest version committed AT OR BEFORE it, per table — with a
    clamp: a time after the table's last commit gives its latest version. (This is why a single
    global timestamp must NOT be passed straight to `TIMESTAMP AS OF`: each table has its own commit
    timeline, and Delta RAISES `DELTA_TIMESTAMP_GREATER_THAN_COMMIT` for a time past a table's last
    commit — e.g. a source not rewritten by the run being replayed.)"""
    if isinstance(when, int):
        return when
    row = spark.sql(f"SELECT max(version) v FROM (DESCRIBE HISTORY {fqn}) "
                    f"WHERE timestamp <= '{when}'").collect()[0]
    if row[0] is None:
        raise ValueError(f"{fqn}: no version at or before {when}")
    return int(row[0])


def prepare_flow_sandbox(spark, *, catalog: str, src_schema: str, tables: list[str],
                         orig_schema: str, opt_schema: str, sample_percent: float | None = None,
                         sample_only=None, seed: int = 42) -> dict:
    """Clone every table into BOTH schemas from one snapshot; _orig and _opt come out identical.

    `sample_only` (table names) get `TABLESAMPLE ... REPEATABLE(seed)` — same seed => identical on
    both sides — so a huge driver stays cheap; every other table is a full SHALLOW CLONE (preserves
    partitioning, required for partition-scoped reloads). Never DROP (CREATE OR REPLACE only).
    """
    sample_only = set(sample_only or ())
    for sch in (orig_schema, opt_schema):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{sch}")
    made = {"orig": [], "opt": []}
    for t in tables:
        src = f"{catalog}.{src_schema}.{t}"
        for sch, key in ((orig_schema, "orig"), (opt_schema, "opt")):
            dst = f"{catalog}.{sch}.{t}"
            if sample_percent and t in sample_only:
                spark.sql(f"CREATE OR REPLACE TABLE {dst} AS SELECT * FROM {src} "
                          f"TABLESAMPLE ({sample_percent} PERCENT) REPEATABLE ({seed})")
            else:
                spark.sql(f"CREATE OR REPLACE TABLE {dst} SHALLOW CLONE {src}")
            made[key].append(dst)
    return made


def _poll(w, run_id, *, timeout_s, poll_s) -> dict:
    """Wait a submitted run to a terminal state; return result + per-task result_states."""
    deadline = time.time() + timeout_s
    while True:
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        state = run.get("state", {})
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        if time.time() > deadline:
            raise TimeoutError(f"run {run_id} not done after {timeout_s}s "
                               f"(state {state.get('life_cycle_state')})")
        time.sleep(poll_s)
    per_task = {t["task_key"]: t.get("state", {}).get("result_state") for t in run.get("tasks", [])}
    return {"run_id": run_id, "result_state": state.get("result_state"),
            "state_message": state.get("state_message"),
            "run_page_url": run.get("run_page_url"), "tasks": per_task}


def submit_job_to_schema(job_id, *, catalog: str, schema: str, cluster_id: str,
                         run_name: str | None = None, timeout_s: int = 3600, poll_s: int = 15) -> dict:
    """Run a job's WHOLE DAG once against `catalog.schema`, on `cluster_id`.

    Rebuilds the task list from the stored job (task_key, notebook path, depends_on), injects
    base_parameters {catalog, schema} into EVERY notebook task (the original job leaves some empty,
    which would default to prod), and pins existing_cluster_id. `runs/submit` is ephemeral — it
    never mutates the stored job — and prod is untouched because all writes land in `schema`.
    """
    w = WorkspaceClient()
    stg = w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]
    tasks = []
    for t in stg["tasks"]:
        nt = t.get("notebook_task")
        if not nt:
            raise ValueError(f"task {t['task_key']} is not a notebook task; "
                             "flow-validate handles notebook DAGs only")
        params = dict(nt.get("base_parameters") or {})
        params.update({"catalog": catalog, "schema": schema})
        task = {"task_key": t["task_key"], "existing_cluster_id": cluster_id,
                "notebook_task": {"notebook_path": nt["notebook_path"], "base_parameters": params}}
        if t.get("depends_on"):
            task["depends_on"] = t["depends_on"]
        tasks.append(task)
    body = {"run_name": run_name or f"flow-validate {schema}", "tasks": tasks}
    run_id = w.api_client.do("POST", "/api/2.2/jobs/runs/submit", body=body)["run_id"]
    return _poll(w, run_id, timeout_s=timeout_s, poll_s=poll_s)


def compare_flow(spark, *, catalog: str, targets: list[str], orig_schema: str, opt_schema: str,
                 risk_tier: str = "semantics_preserving", partition_cols=None) -> dict:
    """Compare each final target _orig vs _opt with the equivalence ladder. Collects a per-table
    verdict (does not stop at the first divergence, so the report is complete)."""
    results, all_pass = {}, True
    for t in targets:
        a, b = f"{catalog}.{orig_schema}.{t}", f"{catalog}.{opt_schema}.{t}"
        pcols = (partition_cols or {}).get(t)
        try:
            results[t] = {"passed": True,
                          "detail": equivalence.assert_equivalent(spark, a, b, risk_tier=risk_tier,
                                                                   partition_cols=pcols)}
        except Exception as e:
            all_pass = False
            results[t] = {"passed": False, "reason": str(e)}
    return {"passed": all_pass, "targets": results}


def flow_validate(spark, *, job_name: str, orig_job_id, opt_job_id, catalog: str, src_schema: str,
                  tables: list[str], targets: list[str], cluster_id: str,
                  sample_percent: float | None = None, sample_only=None, seed: int = 42,
                  partition_cols=None, risk_tier: str = "semantics_preserving",
                  run_timeout_s: int = 3600) -> dict:
    """End-to-end flow gate: prepare -> run both jobs -> compare -> audit flow_equivalence.

    Returns a verdict dict (no RAISE on a clean equivalence FAIL — a HITL gate decides
    promote/rollback); RAISES only on an operational failure (a job run that did not SUCCEED, etc.).
    """
    orig_schema, opt_schema = flow_schema_names(src_schema)
    audit_log(spark, job=job_name, notebook="<flow>", step="flow_equivalence", status="started",
              insight=f"Flow validate: orig job {orig_job_id} vs opt job {opt_job_id}; "
                      f"{('sampled '+str(sample_percent)+'%') if sample_percent else 'full'} start, seed {seed}.")
    try:
        prepare_flow_sandbox(spark, catalog=catalog, src_schema=src_schema, tables=tables,
                             orig_schema=orig_schema, opt_schema=opt_schema,
                             sample_percent=sample_percent, sample_only=sample_only, seed=seed)
        orig_run = submit_job_to_schema(orig_job_id, catalog=catalog, schema=orig_schema,
                                        cluster_id=cluster_id, run_name=f"flow orig {job_name}",
                                        timeout_s=run_timeout_s)
        opt_run = submit_job_to_schema(opt_job_id, catalog=catalog, schema=opt_schema,
                                       cluster_id=cluster_id, run_name=f"flow opt {job_name}",
                                       timeout_s=run_timeout_s)
        if orig_run["result_state"] != "SUCCESS" or opt_run["result_state"] != "SUCCESS":
            raise RuntimeError(f"job run(s) not SUCCESS: orig={orig_run['result_state']} "
                               f"opt={opt_run['result_state']} "
                               f"({orig_run['run_page_url']} / {opt_run['run_page_url']})")
        cmp = compare_flow(spark, catalog=catalog, targets=targets, orig_schema=orig_schema,
                           opt_schema=opt_schema, risk_tier=risk_tier, partition_cols=partition_cols)
        audit_log(spark, job=job_name, notebook="<flow>", step="flow_equivalence",
                  status="succeeded" if cmp["passed"] else "failed",
                  equivalence={t: ("pass" if r["passed"] else "FAIL") for t, r in cmp["targets"].items()},
                  insight=("Flow equivalence PASSED: every final target of the optimized job matches "
                           "the original job from an identical start."
                           if cmp["passed"] else
                           "Flow equivalence FAILED on: "
                           + ", ".join(t for t, r in cmp["targets"].items() if not r["passed"])))
        return {"passed": cmp["passed"], "orig_run": orig_run, "opt_run": opt_run,
                "comparison": cmp, "orig_schema": orig_schema, "opt_schema": opt_schema}
    except Exception as e:
        audit_log(spark, job=job_name, notebook="<flow>", step="flow_equivalence", status="failed",
                  insight=f"Flow validate errored: {e}")
        raise


# --- Replay against a real past run (ground truth) -------------------------------------------------
# Instead of re-running the original job, reproduce what a REAL past run of the original pipeline
# produced: pin the inputs (sources + the target's pre-run state) to the moment that run started
# (Delta time-travel), run ONLY the optimized job on those exact inputs, and compare its output to
# the original run's RECORDED output. This answers "if the pipeline ran N days ago, does the new
# optimized job give the same end-to-end result?" on the real data as it was — no synthetic data,
# no re-running the original. Requires the anchor versions to still be time-travelable (not VACUUMed).

def prepare_replay_sandbox(spark, *, catalog: str, src_schema: str, replay_schema: str,
                           tables: list[str], anchor_before: str, table_pins=None) -> dict:
    """Materialize each input table into `replay_schema` as it was at `anchor_before` (when the
    original run started). `tables` = every source + the targets (whose PRE-run state the job reads).
    `table_pins` overrides the pin per table: an int -> `VERSION AS OF`, a str -> `TIMESTAMP AS OF`.

    SHALLOW CLONE (zero-copy, disposable, preserves partitioning) — never a full copy. The optimized
    job then runs against THESE clones (copy-on-write), so prod is never touched; drop them after."""
    table_pins = table_pins or {}
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{replay_schema}")
    made = {}
    for t in tables:
        src, dst = f"{catalog}.{src_schema}.{t}", f"{catalog}.{replay_schema}.{t}"
        ver = _resolve_version(spark, src, table_pins.get(t, anchor_before))
        spark.sql(f"CREATE OR REPLACE TABLE {dst} SHALLOW CLONE {src} VERSION AS OF {ver}")
        made[t] = dst
    return made


def compare_to_ground_truth(spark, *, catalog: str, src_schema: str, replay_schema: str,
                            targets: list[str], anchor_after: str, risk_tier="semantics_preserving",
                            partition_cols=None, target_pins=None) -> dict:
    """Compare the replay's output for each target to the original run's RECORDED output — prod
    `TIMESTAMP AS OF anchor_after` (just after the original run finished). Per-target verdict."""
    target_pins, results, all_pass = (target_pins or {}), {}, True
    for t in targets:
        ver = _resolve_version(spark, f"{catalog}.{src_schema}.{t}", target_pins.get(t, anchor_after))
        truth = spark.sql(f"SELECT * FROM {catalog}.{src_schema}.{t} VERSION AS OF {ver}")
        candidate = f"{catalog}.{replay_schema}.{t}"
        pcols = (partition_cols or {}).get(t)
        try:
            results[t] = {"passed": True,
                          "detail": equivalence.assert_equivalent(spark, truth, candidate,
                                                                  risk_tier=risk_tier, partition_cols=pcols)}
        except Exception as e:
            all_pass = False
            results[t] = {"passed": False, "reason": str(e)}
    return {"passed": all_pass, "targets": results}


def replay_validate(spark, *, job_name: str, opt_job_id, catalog: str, src_schema: str,
                    tables: list[str], targets: list[str], cluster_id: str,
                    anchor_before: str, anchor_after: str, replay_schema: str | None = None,
                    table_pins=None, target_pins=None, risk_tier="semantics_preserving",
                    partition_cols=None, run_timeout_s: int = 3600) -> dict:
    """Replay the optimized job on a real past run's inputs and compare to that run's recorded output.

    `anchor_before` = the timestamp the original run started (inputs are pinned there);
    `anchor_after` = a timestamp just after it finished (its outputs are read there). Returns a
    verdict dict; audits `flow_equivalence` (mode=replay). RAISES only on operational failure.
    """
    replay_schema = replay_schema or f"{src_schema}_replay"
    audit_log(spark, job=job_name, notebook="<flow>", step="flow_equivalence", status="started",
              insight=f"Replay validate: opt job {opt_job_id} on inputs @ {anchor_before} vs the "
                      f"original run's recorded output @ {anchor_after}.")
    try:
        prepare_replay_sandbox(spark, catalog=catalog, src_schema=src_schema, replay_schema=replay_schema,
                               tables=tables, anchor_before=anchor_before, table_pins=table_pins)
        opt_run = submit_job_to_schema(opt_job_id, catalog=catalog, schema=replay_schema,
                                       cluster_id=cluster_id, run_name=f"replay {job_name}",
                                       timeout_s=run_timeout_s)
        if opt_run["result_state"] != "SUCCESS":
            raise RuntimeError(f"optimized job run not SUCCESS: {opt_run['result_state']} "
                               f"({opt_run['run_page_url']})")
        cmp = compare_to_ground_truth(spark, catalog=catalog, src_schema=src_schema,
                                      replay_schema=replay_schema, targets=targets,
                                      anchor_after=anchor_after, risk_tier=risk_tier,
                                      partition_cols=partition_cols, target_pins=target_pins)
        audit_log(spark, job=job_name, notebook="<flow>", step="flow_equivalence",
                  status="succeeded" if cmp["passed"] else "failed",
                  equivalence={t: ("pass" if r["passed"] else "FAIL") for t, r in cmp["targets"].items()},
                  insight=("Replay PASSED: the optimized job reproduced the original run's recorded "
                           "output on every target, from identical time-travelled inputs."
                           if cmp["passed"] else
                           "Replay FAILED on: " + ", ".join(t for t, r in cmp["targets"].items()
                                                            if not r["passed"])))
        return {"passed": cmp["passed"], "opt_run": opt_run, "comparison": cmp,
                "replay_schema": replay_schema}
    except Exception as e:
        audit_log(spark, job=job_name, notebook="<flow>", step="flow_equivalence", status="failed",
                  insight=f"Replay validate errored: {e}")
        raise
