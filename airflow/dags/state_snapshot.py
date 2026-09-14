from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

MC_CONTAINER = os.environ.get("P3_MC_CONTAINER", "p3-mc")
SPARK_VERSION_TAG = os.environ.get("P3_SPARK_VERSION_TAG", "v3.5.1")
SNAPSHOT_ROOT = "local/state-snapshots/balance_engine"
CHECKPOINT_SRC = f"local/balance-lake/checkpoints/{SPARK_VERSION_TAG}/"

DEFAULT_ARGS = {
    "owner": "balance-velocity-engine",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # A missed snapshot must never queue up behind itself: a backlog of mirrors
    # against a live checkpoint is a good way to turn a slow disk into an outage.
    "depends_on_past": False,
}


def push_snapshot_lag(**context):
    """snapshot_lag_seconds = now - newest snapshot timestamp.

    The metric the runbook alerts on. It rises steadily between runs and drops to
    near zero on each success, so a flat rising line means the DAG has stopped and
    the corruption recovery window is silently growing.
    """
    import subprocess
    import time

    out = subprocess.run(
        ["docker", "exec", MC_CONTAINER, "mc", "ls", f"{SNAPSHOT_ROOT}/"],
        capture_output=True, text=True, timeout=60)
    stamps = []
    for line in out.stdout.splitlines():
        token = line.strip().split("/")[-2] if "/" in line else line.strip().split()[-1]
        token = token.strip("/")
        try:
            stamps.append(datetime.strptime(token, "%Y%m%dT%H%M%SZ"))
        except ValueError:
            continue

    lag = 0.0 if not stamps else (
        datetime.utcnow() - max(stamps)).total_seconds()

    try:
        import sys
        sys.path.insert(0, os.environ.get("P3_REPO_ROOT", "/opt/balance-velocity-engine"))
        from spark.engine.metrics import METRIC_SNAPSHOT_LAG, emit
        emit({METRIC_SNAPSHOT_LAG: lag, "snapshots": len(stamps)}, job="state_snapshot")
    except Exception:
        pass
    print(f"snapshot_lag_seconds={lag:.0f} across {len(stamps)} snapshot(s)")
    return lag


with DAG(
    dag_id="p3_state_snapshot",
    description="Immutable versioned copies of the balance engine's durable checkpoint",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",          # demo: 5 min. production: */30
    catchup=False,                    # never backfill a snapshot; only now matters
    max_active_runs=1,                # a mirror must not overlap itself
    tags=["project3", "state", "disaster-recovery"],
) as dag:

    snapshot = BashOperator(
        task_id="snapshot_state",
        bash_command=(
            f'set -euo pipefail; '
            f'TS=$(date -u +%Y%m%dT%H%M%SZ); '
            f'docker exec {MC_CONTAINER} mc mirror --overwrite '
            f'  "{CHECKPOINT_SRC}" "{SNAPSHOT_ROOT}/$TS/" >/dev/null; '
            f'echo "snapshot $TS"; '
            f'docker exec {MC_CONTAINER} mc ls "{SNAPSHOT_ROOT}/" | tail -5'
        ),
    )

    lag = PythonOperator(
        task_id="push_snapshot_lag",
        python_callable=push_snapshot_lag,
    )

    # 7-day retention in production; the demo keeps the newest 12 so MinIO on a
    # laptop does not fill up during a day of drills.
    retention = BashOperator(
        task_id="prune_old_snapshots",
        bash_command=(
            f'set -euo pipefail; '
            f'KEEP=12; '
            f'ALL=$(docker exec {MC_CONTAINER} mc ls "{SNAPSHOT_ROOT}/" '
            f'      | awk "{{print \\$NF}}" | tr -d "/" | sort); '
            f'N=$(echo "$ALL" | wc -l); '
            f'if [ "$N" -gt "$KEEP" ]; then '
            f'  echo "$ALL" | head -n $((N-KEEP)) | while read s; do '
            f'    docker exec {MC_CONTAINER} mc rm -r --force "{SNAPSHOT_ROOT}/$s/" >/dev/null; '
            f'    echo "pruned $s"; done; '
            f'else echo "nothing to prune ($N <= $KEEP)"; fi'
        ),
    )

    snapshot >> lag >> retention
