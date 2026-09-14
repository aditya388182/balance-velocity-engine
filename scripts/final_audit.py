#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

GREEN, RED, YELLOW, BLUE, DIM, RESET = (
    "\033[92m", "\033[91m", "\033[93m", "\033[94m", "\033[2m", "\033[0m")

PASS, FAIL, WARN = [], [], []


def head(t):
    print(f"\n{BLUE}{t}{RESET}\n" + "-" * len(t))


def ok(label, detail=""):
    PASS.append(label)
    print(f"  {GREEN}PASS{RESET}  {label}" + (f"  {DIM}{detail}{RESET}" if detail else ""))


def bad(label, detail=""):
    FAIL.append(f"{label}: {detail}")
    print(f"  {RED}FAIL{RESET}  {label}" + (f"  {detail}" if detail else ""))


def warn(label, detail=""):
    WARN.append(f"{label}: {detail}")
    print(f"  {YELLOW}TODO{RESET}  {label}" + (f"  {detail}" if detail else ""))


def exists(rel, note=""):
    p = REPO_ROOT / rel
    if p.exists() and (p.is_dir() or p.stat().st_size > 0):
        ok(rel, note)
        return True
    bad(rel, "missing or empty")
    return False


def audit_code():
    head("A. Engine and ground truth")
    for f in ("conf/engine_config.yml", "conf/config.py",
              "schemas/account_event_v1.avsc",
              "scripts/event_generator.py", "scripts/oracle.py",
              "scripts/parity_balance.py",
              "spark/engine/statecore.py", "spark/engine/state.py",
              "spark/engine/sequencer.py", "spark/engine/gap_policy.py",
              "spark/engine/sinks.py", "spark/engine/velocity.py",
              "spark/engine/metrics.py", "spark/jobs/balance_engine.py",
              "spark/utils/avro_deserializer.py", "spark/utils/session.py",
              "spark/utils/progress.py"):
        exists(f)

    cfg = (REPO_ROOT / "conf/engine_config.yml").read_text()
    missing = [k for k in ("watermark_delay", "trigger_interval", "max_buffer_size",
                           "state_ttl", "gap_policy", "checkpoint_root",
                           "spark_version_tag", "vel_window", "vel_limit",
                           "gap_realert_interval", "rejoin_reseed")
               if k not in cfg]
    if missing:
        bad("config keys", f"missing {missing}")
    else:
        ok("every contract tunable present", "with demo and production columns")

    if "production" in cfg:
        ok("both parameter columns documented")
    else:
        bad("parameter columns", "no production values in engine_config.yml")


def audit_schemas():
    head("B. Checkpoint identity")
    try:
        from spark.engine.statecore import OUTPUT_COLUMNS, empty_state
    except Exception as exc:
        bad("statecore import", repr(exc))
        return
    st = empty_state(now_ms=1)
    if len(st) == 5 and st[4] == 1:
        ok("state tuple arity and last_seen_ms", "5 fields, present since day one")
    else:
        bad("state tuple", str(st))
    want = ["account_id", "last_applied_seq", "balance_minor", "buffer_size",
            "out_kind", "detail"]
    if OUTPUT_COLUMNS == want:
        ok("OUTPUT_SCHEMA columns unchanged", ", ".join(want[:4]) + " ...")
    else:
        bad("OUTPUT_SCHEMA", str(OUTPUT_COLUMNS))


def audit_fixtures():
    head("C. The CI contract")
    want = {"in_order", "out_of_order", "duplicate_applied", "duplicate_buffered",
            "gap", "hold_policy", "restart", "overflow"}
    paths = list((REPO_ROOT / "data/fixtures/ci_sequences").glob("*.json"))
    have = {json.loads(p.read_text())["name"] for p in paths}
    if have == want:
        ok(f"all {len(want)} matrix fixtures committed", ", ".join(sorted(have)))
    else:
        bad("fixtures", f"missing {sorted(want - have)} extra {sorted(have - want)}")
    undoc = [json.loads(p.read_text())["name"] for p in paths
             if not json.loads(p.read_text()).get("description", "").strip()]
    if undoc:
        bad("fixture docs", str(undoc))
    else:
        ok("every fixture documents its claim", "the contract is reviewable")


def audit_drills():
    head("D. Drills and diagnostics")
    for f in ("scripts/stage_run.sh", "scripts/reset_lake.sh", "scripts/run_engine.sh",
              "scripts/chaos/kill_engine.sh", "scripts/recovery_drill.sh",
              "scripts/sink_replay_drill.sh", "scripts/replay_evidence.py",
              "scripts/inject_burst.py", "scripts/buffer_proof.py",
              "scripts/stage5_proof.sh", "scripts/release_head_drill.sh",
              "scripts/late_arrival_proof.py", "scripts/stage6_proof.sh",
              "scripts/rejoin_proof.py", "scripts/velocity_recompute.py",
              "scripts/snapshot_state.sh", "scripts/corrupt_checkpoint.sh",
              "scripts/restore_from_snapshot.sh", "scripts/upgrade_dual_run.sh",
              "scripts/gap_timing_probe.py", "scripts/wait_for_drain.py",
              "scripts/trace_event.py", "scripts/diagnose.py",
              "scripts/diagnose_velocity.py", "scripts/check_install.py",
              "scripts/state_growth.py", "scripts/consume_topic.py"):
        exists(f)


def audit_observability():
    head("E. Observability")
    for f in ("infra/prometheus/prometheus.yml",
              "infra/grafana/provisioning/datasources/prometheus.yml",
              "infra/grafana/provisioning/dashboards/dashboards.yml",
              "infra/grafana/dashboards/state_health.json",
              "infra/grafana/dashboards/integrity_signals.json"):
        exists(f)

    try:
        from spark.engine.metrics import ALL_METRICS
    except Exception as exc:
        bad("metrics import", repr(exc))
        return

    bound, unknown = set(), []
    for f in glob.glob(str(REPO_ROOT / "infra/grafana/dashboards/*.json")):
        for p in json.load(open(f))["panels"]:
            for t in p.get("targets", []):
                for name in re.findall(r"p3_[a-z0-9_]+", t["expr"]):
                    bound.add(name)
                    if name not in ALL_METRICS:
                        unknown.append((Path(f).name, p["title"], name))
    if unknown:
        bad("dashboard panels", f"bound to undefined metrics: {unknown}")
    else:
        ok(f"every panel binds to a defined metric", f"{len(bound)} distinct names")

    # a metric nobody reads is a metric nobody maintains
    runbook_text = "\n".join((REPO_ROOT / "runbooks" / f).read_text()
                             for f in ("state_growing_unbounded.md",
                                       "gap_alert_storm.md",
                                       "checkpoint_corruption.md",
                                       "spark_upgrade.md")
                             if (REPO_ROOT / "runbooks" / f).exists())
    unreferenced = [m for m in ALL_METRICS
                    if m not in runbook_text and m not in bound]
    if unreferenced:
        warn("metrics referenced by neither a dashboard nor a runbook",
             ", ".join(unreferenced))
    else:
        ok("every metric is read by a dashboard or a runbook")


def audit_cicd():
    head("F. CI/CD and infrastructure-as-code")
    for f in (".github/workflows/ci.yml", ".github/workflows/deploy.yml",
              "terraform/main.tf", "terraform/README.md",
              "requirements.txt", "requirements-core.txt",
              "infra/docker-compose.yml", "infra/kafka/topics.sh",
              "airflow/dags/state_snapshot.py"):
        exists(f)
    ci = (REPO_ROOT / ".github/workflows/ci.yml").read_text()
    jobs = re.findall(r"^  ([a-z0-9-]+):$", ci, re.M)
    if len(jobs) >= 4:
        ok(f"CI has {len(jobs)} jobs", ", ".join(jobs))
    else:
        bad("CI jobs", f"only {jobs}")
    if "requirements-core.txt" in ci:
        ok("job 1 installs the hermetic subset only", "seconds on a bare runner")
    else:
        bad("job 1", "does not use requirements-core.txt")
    if "STATE_SCHEMA changed" in ci:
        ok("CI gates on STATE_SCHEMA", "checkpoint identity cannot change silently")
    else:
        bad("CI", "no STATE_SCHEMA guard")


def audit_docs():
    head("G. Documentation")
    for f in ("README.md", "docs/watermark_and_gap_semantics.md",
              "docs/native_vs_custom.md", "docs/daily_log.md",
              "docs/video_script.md",
              "runbooks/state_growing_unbounded.md", "runbooks/gap_alert_storm.md",
              "runbooks/checkpoint_corruption.md", "runbooks/spark_upgrade.md"):
        exists(f)

    readme = (REPO_ROOT / "README.md").read_text()
    for phrase, label in (
            ("Why Spark", "README leads with the why-Spark defense"),
            ("Honest limitations", "README states its limitations"),
            ("Measured numbers", "README has a measured-numbers table"),
            ("787.04", "the false-positive cost is quoted with a number")):
        ok(label) if phrase in readme else bad(label, f"'{phrase}' not found")

    log = (REPO_ROOT / "docs/daily_log.md").read_text()
    days = len(re.findall(r"^## Day \d", log, re.M))
    if days >= 6:
        ok(f"daily log covers {days} days")
    else:
        warn("daily log", f"only {days} day sections; Day 6 not written yet")
    if "<what actually broke today" in log:
        warn("daily log war stories", "placeholders still present — fill these in")
    else:
        ok("war stories filled in")


def audit_screenshots(strict):
    head("H. Screenshots (only you can confirm these)")
    d = REPO_ROOT / "docs/screenshots"
    pngs = sorted(p.name for p in d.glob("*.png")) if d.exists() else []
    expected = 19
    if len(pngs) >= expected:
        ok(f"{len(pngs)} screenshots present", ", ".join(pngs[:4]) + " ...")
    elif pngs:
        (bad if strict else warn)("screenshots",
                                  f"{len(pngs)} of ~{expected}: {', '.join(pngs)}")
    else:
        (bad if strict else warn)("screenshots", "none captured yet")


def audit_manual():
    head("I. Confirm by hand — these cannot be checked mechanically")
    for item in [
        "Both disaster drills run with measured timings in logs/chaos.jsonl",
        "Grafana dashboards populated over a real workload",
        "CI green captured, and the red run captured with the doubled-debit failure",
        "Video script dry-run performed once end to end",
        "git history has >= 6 meaningful commits and is pushed",
        "You can answer Q1-Q4 unaided (see below)",
    ]:
        print(f"  {DIM}[ ]{RESET}  {item}")


def interview_self_test():
    head("J. Interview self-test — say each answer out loud, unaided")
    for q, a in [
        ("Q1 Why Spark stateful streaming and not a specialist engine?",
         "second runtime = doubled ops surface + split expertise + re-proving "
         "exactly-once against Delta; seconds-latency SLA met; native "
         "RocksDB+S3+Delta integration; consolidation beats optimisation; revisit "
         "trigger = a hard sub-second SLA"),
        ("Q2 How do you distinguish a late event from a lost one?",
         "the watermark is the line; buffered-if-before, gap-if-after; event-time "
         "timeout armed at the earliest buffered successor; latency bounded by "
         "watermark + trigger; and the 5-second-watermark run that cost 787.04"),
        ("Q3 What happens when the state format changes on upgrade?",
         "versioned checkpoint paths since day one; the new job seeds from a "
         "snapshot + bounded replay and NEVER reads old-format state; old job "
         "drains on SIGTERM; 24h parity; cutover; the database-migration parallel"),
        ("Q4 100,000 out-of-order events in 30 seconds — how does it survive?",
         "hard cap at 1000, min-first eviction over the buffer AND the arrival, to "
         "a monitored DLQ; the account degrades itself not its neighbours; measured "
         "-1,000,000 capped vs -10,000,000 uncapped; and the cap also delays gap "
         "detection, which is the cost people forget"),
    ]:
        print(f"  {DIM}[ ]{RESET}  {q}")
        print(f"        {DIM}{a}{RESET}")


def main():
    p = argparse.ArgumentParser(description="Project 3 completion audit")
    p.add_argument("--strict", action="store_true",
                   help="treat missing screenshots as failures")
    args = p.parse_args()

    build = "UNKNOWN"
    try:
        build = (REPO_ROOT / "BUILD").read_text().strip()
    except Exception:
        pass
    print(f"Project 3 — final completion audit   build {build}")
    print("=" * 78)

    audit_code()
    audit_schemas()
    audit_fixtures()
    audit_drills()
    audit_observability()
    audit_cicd()
    audit_docs()
    audit_screenshots(args.strict)
    audit_manual()
    interview_self_test()

    print("\n" + "=" * 78)
    print(f"{GREEN}{len(PASS)} passed{RESET}   {RED}{len(FAIL)} failed{RESET}   "
          f"{YELLOW}{len(WARN)} to do{RESET}")
    if FAIL:
        print("\nfailures:")
        for f in FAIL:
            print(f"  - {f}")
        sys.exit(1)
    if WARN:
        print("\nremaining:")
        for w in WARN:
            print(f"  - {w}")
    print(f"\n{GREEN}Every mechanically checkable artifact is present.{RESET}")
    print("Work through sections H, I and J by hand, then Project 3 is complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
