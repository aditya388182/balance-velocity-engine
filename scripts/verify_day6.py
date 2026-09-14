#!/usr/bin/env python3
from __future__ import annotations

import glob
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import yaml  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"
PASSES, FAILS = [], []


def ok(label, detail=""):
    PASSES.append(label)
    print(f"  {GREEN}PASS{RESET}  {label}" + (f"  {DIM}{detail}{RESET}" if detail else ""))


def fail(label, detail=""):
    FAILS.append(f"{label}: {detail}")
    print(f"  {RED}FAIL{RESET}  {label}" + (f"  {detail}" if detail else ""))


def section(t):
    print(f"\n{t}\n" + "-" * len(t))


def check_parses():
    section("1. Everything parses")
    for f in ("infra/docker-compose.yml", "infra/prometheus/prometheus.yml",
              "infra/grafana/provisioning/datasources/prometheus.yml",
              "infra/grafana/provisioning/dashboards/dashboards.yml",
              "conf/engine_config.yml",
              ".github/workflows/ci.yml", ".github/workflows/deploy.yml"):
        try:
            yaml.safe_load((REPO_ROOT / f).read_text())
            ok(f"YAML {f}")
        except Exception as exc:
            fail(f"YAML {f}", repr(exc))
    for f in glob.glob(str(REPO_ROOT / "infra/grafana/dashboards/*.json")):
        try:
            d = json.loads(Path(f).read_text())
            assert d["panels"]
            ok(f"JSON {Path(f).name}", f"{len(d['panels'])} panels")
        except Exception as exc:
            fail(f"JSON {Path(f).name}", repr(exc))


def check_compose_wiring():
    section("2. The observability stack is actually wired together")
    d = yaml.safe_load((REPO_ROOT / "infra/docker-compose.yml").read_text())
    svc = d["services"]
    for name in ("pushgateway", "prometheus", "grafana"):
        if name in svc:
            ok(f"service {name} enabled")
        else:
            fail(f"service {name}", "still commented out")

    prom = yaml.safe_load((REPO_ROOT / "infra/prometheus/prometheus.yml").read_text())
    targets = [t for sc in prom["scrape_configs"]
               for cfg in sc.get("static_configs", []) for t in cfg["targets"]]
    if any("pushgateway" in t for t in targets):
        ok("Prometheus scrapes the pushgateway", ", ".join(targets))
    else:
        fail("Prometheus targets", str(targets))

    if any(sc.get("honor_labels") for sc in prom["scrape_configs"]):
        ok("honor_labels set", "the pusher's job label survives the scrape")
    else:
        fail("honor_labels", "pushed metrics will be relabelled to 'pushgateway'")

    ds = yaml.safe_load(
        (REPO_ROOT / "infra/grafana/provisioning/datasources/prometheus.yml").read_text())
    uid = ds["datasources"][0].get("uid")
    panel_uids = set()
    for f in glob.glob(str(REPO_ROOT / "infra/grafana/dashboards/*.json")):
        for p in json.load(open(f))["panels"]:
            panel_uids.add(p["datasource"]["uid"])
    if panel_uids == {uid}:
        ok("dashboard panels resolve to the provisioned datasource", f"uid={uid}")
    else:
        fail("datasource uid", f"panels use {panel_uids}, provisioning declares {uid}")


def check_metric_references():
    section("3. Nothing points at a metric that does not exist")
    from spark.engine.metrics import ALL_METRICS

    bad = []
    used = set()
    for f in glob.glob(str(REPO_ROOT / "infra/grafana/dashboards/*.json")):
        for p in json.load(open(f))["panels"]:
            for t in p.get("targets", []):
                for name in re.findall(r"p3_[a-z0-9_]+", t["expr"]):
                    used.add(name)
                    if name not in ALL_METRICS:
                        bad.append((Path(f).name, p["title"], name))
    if bad:
        fail("dashboard metric names", str(bad))
    else:
        ok(f"{len(used)} dashboard metric names all defined in metrics.py")

    rb_bad = []
    rb_used = set()
    for f in glob.glob(str(REPO_ROOT / "runbooks/*.md")):
        for name in re.findall(r"p3_[a-z0-9_]+", Path(f).read_text()):
            rb_used.add(name)
            if name not in ALL_METRICS:
                rb_bad.append((Path(f).name, name))
    if rb_bad:
        fail("runbook metric names", str(rb_bad))
    else:
        ok(f"{len(rb_used)} runbook metric names all defined",
           "a runbook naming a metric nobody emits fails during the incident")


def check_internal_links():
    section("4. Every internal link resolves")
    broken = []
    checked = 0
    for f in ["README.md"] + glob.glob(str(REPO_ROOT / "runbooks/*.md")) \
             + glob.glob(str(REPO_ROOT / "docs/*.md")):
        path = REPO_ROOT / f if not str(f).startswith("/") else Path(f)
        if not path.exists():
            continue        # its absence is reported by the section that owns it
        text = path.read_text()
        # strip fenced blocks and inline code first: a doc that TALKS ABOUT markdown
        # links contains link-shaped text that is not a link.
        text = re.sub(r"```.*?```", "", text, flags=re.S)
        text = re.sub(r"`[^`]*`", "", text)
        for target in re.findall(r"\]\(([^)#]+?)\)", text):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            checked += 1
            resolved = (path.parent / target).resolve()
            if not resolved.exists():
                broken.append((path.name, target))
    if broken:
        fail("internal links", str(broken[:6]))
    else:
        ok(f"{checked} internal links resolve")


def check_scripts_referenced_exist():
    section("5. Every script a drill or runbook invokes exists")
    broken = set()
    checked = set()
    sources = (glob.glob(str(REPO_ROOT / "scripts/*.sh"))
               + glob.glob(str(REPO_ROOT / "scripts/chaos/*.sh"))
               + glob.glob(str(REPO_ROOT / "runbooks/*.md"))
               + [str(REPO_ROOT / "README.md")])
    for f in sources:
        if not Path(f).exists():
            continue        # its absence is reported by the section that owns it
        text = Path(f).read_text()
        for m in re.findall(r"(?:\./)?(scripts/[a-z_0-9/]+\.(?:py|sh))", text):
            checked.add(m)
            if not (REPO_ROOT / m).exists():
                broken.add((Path(f).name, m))
    if broken:
        fail("referenced scripts", str(sorted(broken)[:6]))
    else:
        ok(f"{len(checked)} referenced scripts all exist")


def check_ci_shape():
    section("6. The CI pipeline does what it claims")
    ci_path = REPO_ROOT / ".github/workflows/ci.yml"
    if not ci_path.exists():
        fail("CI workflow missing", f"{ci_path.relative_to(REPO_ROOT)} does not exist")
        print(f"  {DIM}.github is a DOT directory: Finder hides it, and some GUI "
              f"unzip tools skip it entirely. Extract with `unzip` in a terminal, "
              f"then confirm with `ls -la .github/workflows/`.{RESET}")
        return
    ci_text = ci_path.read_text()
    try:
        ci = yaml.safe_load(ci_text)
    except Exception as exc:
        fail("CI workflow does not parse", repr(exc))
        return
    jobs = list(ci["jobs"])
    if len(jobs) >= 4:
        ok(f"{len(jobs)} jobs", ", ".join(jobs))
    else:
        fail("job count", str(jobs))

    job1 = yaml.safe_dump(ci["jobs"][jobs[0]])
    if "requirements-core.txt" in job1:
        ok("job 1 is hermetic", "installs the core subset, no services")
    else:
        fail("job 1", "does not install requirements-core.txt")

    tests = re.findall(r"spark/tests/(test_[a-z_0-9]+\.py)", ci_text)
    missing = [t for t in set(tests) if not (REPO_ROOT / "spark/tests" / t).exists()]
    if missing:
        fail("CI names tests that do not exist", str(missing))
    else:
        ok(f"all {len(set(tests))} test files named in CI exist")

    if "STATE_SCHEMA changed" in ci_text:
        ok("CI guards checkpoint identity", "a schema change cannot merge quietly")
    else:
        fail("CI", "no STATE_SCHEMA guard")

    for f in re.findall(r"data/fixtures/[a-z_/]+", ci_text):
        if (REPO_ROOT / f).exists():
            ok(f"CI fixture path exists: {f}")
        else:
            fail("CI fixture path", f)


def check_runbook_quality():
    section("7. Runbooks are written from lived drills, not templates")
    for name, must in (
        ("state_growing_unbounded.md", ["p3_buffer_p99", "watermark", "DLQ"]),
        ("gap_alert_storm.md", ["partition lag", "coalesced", "BUFFER_OVERFLOW"]),
        ("checkpoint_corruption.md", ["snapshot IS NOT", "bounded", "throttled"]),
        ("spark_upgrade.md", ["NEVER reads old-format state", "24 hour", "SIGTERM"]),
    ):
        p = REPO_ROOT / "runbooks" / name
        if not p.exists():
            fail(f"runbook {name}", "missing")
            continue
        text = p.read_text()
        missing = [m for m in must if m.lower() not in text.lower()]
        if missing:
            fail(f"runbook {name}", f"does not cover {missing}")
        else:
            ok(f"runbook {name}", f"{len(text.splitlines())} lines, covers its decision tree")


def check_readme():
    section("8. The README leads with judgment")
    rp = REPO_ROOT / "README.md"
    if not rp.exists():
        fail("README.md missing")
        return
    r = rp.read_text()
    idx_why = r.find("Why Spark")
    idx_arch = r.find("## Architecture")
    if 0 < idx_why < idx_arch:
        ok("the why-Spark defense precedes the architecture",
           "this project leads with judgment, not machinery")
    else:
        fail("README order", "the why-Spark section is missing or after the diagram")
    for phrase, label in (("Honest limitations", "limitations stated"),
                          ("Measured numbers", "measured-numbers table"),
                          ("787.04", "the false-positive cost is a number"),
                          ("trace_event", "the diagnostic is discoverable")):
        ok(label) if phrase in r else fail(label, f"'{phrase}' missing")


def main() -> None:
    print("Project 3 — Day 6 verification (hermetic: no Docker, no Kafka, no JVM)")
    print("=" * 74)
    # Each section is isolated. A verifier that stops at the first missing file
    # tells you about one problem when it could have told you about all of them.
    for fn in (check_parses, check_compose_wiring, check_metric_references,
               check_internal_links, check_scripts_referenced_exist,
               check_ci_shape, check_runbook_quality, check_readme):
        try:
            fn()
        except Exception as exc:
            fail(f"section {fn.__name__} raised", repr(exc))

    print("\n" + "=" * 74)
    print(f"{GREEN}{len(PASSES)} passed{RESET}   {RED}{len(FAILS)} failed{RESET}")
    if FAILS:
        print("\nfailures:")
        for f in FAILS:
            print(f"  - {f}")
        sys.exit(1)
    print("\nDay 6 artifacts consistent. Run the drills, then scripts/final_audit.py.")
    sys.exit(0)


if __name__ == "__main__":
    main()
