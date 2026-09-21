"""Offline management for generic Taldau batches; launch is the only scheduling action."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import uuid

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))
from taldau_elt.statistics import batch_summary, create_batch, publish_snapshot, snapshot_summary


def connection():
    return psycopg2.connect(host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5434"),
        dbname=os.getenv("PGDATABASE", "taldau"), user=os.getenv("PGUSER", "taldau"),
        password=os.getenv("PGPASSWORD"), connect_timeout=10)


def airflow_command() -> list[str]:
    return shlex.split(os.environ["TALDAU_AIRFLOW_COMMAND"]) if os.getenv("TALDAU_AIRFLOW_COMMAND") else [
        "docker", "compose", "exec", "-T", "airflow-scheduler", "airflow"]


def launch(batch_id: str, resume_failed: bool) -> None:
    base = airflow_command()
    result = subprocess.run(base + ["pools", "list", "-o", "json"], cwd=ROOT,
                            capture_output=True, text=True, check=True)
    pools = [row for row in json.loads(result.stdout) if row["pool"] == "taldau_api"]
    if len(pools) != 1 or int(pools[0]["slots"]) != 3:
        raise RuntimeError("Global Airflow pool taldau_api must exist with exactly 3 slots")
    subprocess.run(base + ["dags", "unpause", "taldau_statistics_2023_2026"], cwd=ROOT, check=True)
    conf = {"batch_id": batch_id, "allow_extraction": True, "resume_failed": resume_failed,
            "snapshot_overrides": {}}
    subprocess.run(base + ["dags", "trigger", "taldau_statistics_2023_2026", "--run-id",
        batch_id + "__manual__" + uuid.uuid4().hex, "--conf", json.dumps(conf)], cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["migrate", "prepare", "status", "snapshot", "publish", "launch"])
    parser.add_argument("--batch-id")
    parser.add_argument("--snapshot-id")
    parser.add_argument("--resume-failed", action="store_true")
    parser.add_argument("--confirm-extraction", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.action in ("prepare", "status", "launch") and not args.batch_id:
        parser.error("--batch-id is required")
    if args.action in ("snapshot", "publish") and not args.snapshot_id:
        parser.error("--snapshot-id is required")
    if args.action == "launch" and not args.confirm_extraction:
        parser.error("No launch: pass --confirm-extraction only after review")
    conn = connection()
    try:
        if args.action in ("status", "snapshot"):
            conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        if args.action == "migrate":
            path = ROOT / "dags" / "taldau_elt" / "sql" / "010_multi_indicator_framework.sql"
            with conn:
                with conn.cursor() as cur:
                    cur.execute(path.read_text(encoding="utf-8"))
            result = {"migration": path.name, "http_requests": 0}
        elif args.action == "prepare":
            result = create_batch(conn, args.batch_id, resume_failed=args.resume_failed)
        elif args.action == "status":
            result = batch_summary(conn, args.batch_id)
        elif args.action == "snapshot":
            result = snapshot_summary(conn, args.snapshot_id)
        elif args.action == "publish":
            result = {"snapshot_id": args.snapshot_id,
                      "published_rows": publish_snapshot(conn, args.snapshot_id)}
        else:
            create_batch(conn, args.batch_id, resume_failed=args.resume_failed)
            launch(args.batch_id, args.resume_failed)
            result = {"batch_id": args.batch_id, "airflow_triggered": True}
        rendered = json.dumps(result, ensure_ascii=False, indent=2, default=str)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
