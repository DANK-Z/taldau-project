"""Primary paused multi-indicator entry point. Extract -> validate -> diagnostics -> STOP."""
from datetime import timedelta
from pathlib import Path
import sys

_DAG_DIR = str(Path(__file__).resolve().parent)
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from taldau_elt.airflow_compat import SkipExistingDagRunOperator


def connection():
    import psycopg2
    from airflow.hooks.base import BaseHook
    item = BaseHook.get_connection("digest_target_db")
    return psycopg2.connect(host=item.host, port=item.port or 5432, dbname=item.schema,
                            user=item.login, password=item.password, connect_timeout=10)


@dag(
    dag_id="taldau_statistics_2023_2026", schedule=None, catchup=False, max_active_runs=1,
    is_paused_upon_creation=True, render_template_as_native_obj=True,
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Almaty"),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    params={"batch_id": Param("", type="string"),
            "allow_extraction": Param(False, type="boolean"),
            "resume_failed": Param(False, type="boolean"),
            "snapshot_overrides": Param({}, type="object")},
    tags=["taldau", "elt", "multi-indicator", "production-entrypoint"],
)
def statistics_2023_2026():
    @task(retries=0)
    def authorize_batch():
        from airflow.operators.python import get_current_context
        from taldau_elt.statistics import create_batch
        params = get_current_context()["params"]
        if params.get("allow_extraction") is not True:
            raise ValueError("Extraction is disabled. Set allow_extraction=true explicitly.")
        conn = connection()
        try:
            result = create_batch(conn, params["batch_id"], year_start=2023, year_end=2026,
                snapshot_overrides=params.get("snapshot_overrides") or {},
                resume_failed=params.get("resume_failed") is True)
            return result["batch_id"]
        finally:
            conn.close()

    @task
    def list_snapshots(batch_id):
        conn = connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""SELECT snapshot_id FROM taldau.bronze_snapshots
                    WHERE batch_id=%s AND state NOT IN ('validated','published') ORDER BY indicator_key""", (batch_id,))
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    @task(pool="taldau_api", pool_slots=1, max_active_tis_per_dag=3,
          execution_timeout=timedelta(hours=12), retries=2)
    def discover(snapshot_id):
        from airflow.operators.python import get_current_context
        from taldau_elt.statistics import discover_snapshot
        conn = connection()
        try:
            discover_snapshot(conn, snapshot_id,
                              allow_http=get_current_context()["params"]["allow_extraction"] is True)
        finally:
            conn.close()

    @task(trigger_rule="all_done")
    def plan_wave(batch_id):
        from taldau_elt.statistics import pending_batch_chunks
        conn = connection()
        try:
            return pending_batch_chunks(conn, batch_id)
        finally:
            conn.close()

    @task(pool="taldau_api", pool_slots=1, max_active_tis_per_dag=3,
          do_xcom_push=False, execution_timeout=timedelta(hours=4), retries=2)
    def load_chunk(chunk_id):
        from airflow.operators.python import get_current_context
        from taldau_elt.statistics import extract_chunk
        conn = connection()
        try:
            extract_chunk(conn, chunk_id,
                          allow_http=get_current_context()["params"]["allow_extraction"] is True)
        finally:
            conn.close()

    @task.branch(trigger_rule="all_done", retries=0)
    def route_wave(batch_id, selected_ids):
        import hashlib
        from airflow.operators.python import get_current_context
        from taldau_elt.statistics import batch_wave_outcome
        conn = connection()
        try:
            outcome = batch_wave_outcome(conn, batch_id, selected_ids)
        finally:
            conn.close()
        if outcome == "continue":
            context = get_current_context()
            next_id = batch_id + "__wave__" + hashlib.sha256(context["run_id"].encode()).hexdigest()[:24]
            context["ti"].xcom_push(key="next_run_id", value=next_id)
            return "continue_batch"
        return "validate_batch"

    @task(task_id="validate_batch", retries=0)
    def validate(batch_id):
        from taldau_elt.statistics import validate_batch
        conn = connection()
        try:
            validate_batch(conn, batch_id)
            return batch_id
        finally:
            conn.close()

    @task(do_xcom_push=False)
    def diagnostics(batch_id):
        import hashlib
        import json
        import logging
        import os
        from taldau_elt.statistics import batch_summary
        conn = connection()
        try:
            report = batch_summary(conn, batch_id)
            name = hashlib.sha256(batch_id.encode()).hexdigest()[:16]
            output = Path(os.getenv("TALDAU_REPORT_DIR", "/opt/airflow/data/reports")) / f"batch_{name}.json"
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            logging.getLogger(__name__).info("Batch diagnostics: %s", output)
        finally:
            conn.close()

    @task(trigger_rule="all_done", do_xcom_push=False)
    def final_batch_summary(batch_id):
        import logging
        from taldau_elt.statistics import batch_summary
        conn = connection()
        try:
            logging.getLogger(__name__).info("Final batch summary: %s", batch_summary(conn, batch_id))
        finally:
            conn.close()

    batch = authorize_batch()
    snapshot_ids = list_snapshots(batch)
    discovered = discover.expand(snapshot_id=snapshot_ids)
    wave = plan_wave(batch)
    discovered >> wave
    mapped = load_chunk.expand(chunk_id=wave)
    route = route_wave(batch, wave)
    wave >> route
    mapped >> route
    continuation = SkipExistingDagRunOperator(
        task_id="continue_batch", trigger_dag_id="taldau_statistics_2023_2026",
        trigger_run_id="{{ ti.xcom_pull(task_ids='route_wave', key='next_run_id') }}",
        conf={"batch_id": "{{ params.batch_id }}", "allow_extraction": True,
              "resume_failed": False, "snapshot_overrides": "{{ params.snapshot_overrides }}"},
        wait_for_completion=False, reset_dag_run=False)
    checked = validate(batch)
    route >> [continuation, checked]
    diagnosed = diagnostics(checked)
    summary = final_batch_summary(batch)
    diagnosed >> summary


statistics_2023_2026()
