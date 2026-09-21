"""Legacy/deprecated investments-only entry point retained for compatibility."""
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
    c = BaseHook.get_connection('digest_target_db')
    return psycopg2.connect(host=c.host,port=c.port or 5432,dbname=c.schema,
                            user=c.login,password=c.password,connect_timeout=10)


@dag(dag_id='taldau_inv_fixed_assets_2025',schedule=None,catchup=False,max_active_runs=1,
     is_paused_upon_creation=True,render_template_as_native_obj=True,
     start_date=pendulum.datetime(2026,9,1,tz='Asia/Almaty'),
     default_args={'retries':2,'retry_delay':timedelta(minutes=2),'retry_exponential_backoff':True},
     params={'snapshot_id':Param('',type='string'),
             'allow_extraction':Param(False,type='boolean',description='Explicitly authorize missing HTTP requests for the frozen snapshot scope.')},
     tags=['taldau','elt','cube','multi-year','country','legacy','deprecated'])
def investments_2025():
    @task(retries=0)
    def authorize_snapshot():
        from airflow.operators.python import get_current_context
        from taldau_elt.snapshots import read_snapshot
        params=get_current_context()['params']
        if params.get('allow_extraction') is not True:
            raise ValueError('Full extraction is disabled. Explicit approval is required before launch.')
        conn=connection()
        try:
            snapshot=read_snapshot(conn,params['snapshot_id'])
            if not 2023 <= (snapshot['year_start'] or snapshot['year']) <= (snapshot['year_end'] or snapshot['year']) <= 2026:
                raise ValueError('Unsupported year range')
            if snapshot['state'] in ('validated','published'):
                raise ValueError('Snapshot is already complete; do not restart extraction')
            return snapshot['snapshot_id']
        finally:
            conn.close()

    @task(pool='taldau_api',pool_slots=1,execution_timeout=timedelta(hours=12))
    def discover(snapshot_id):
        from taldau_elt.snapshots import discover_territories
        conn=connection()
        try:
            discover_territories(conn,snapshot_id)
            return snapshot_id
        finally:
            conn.close()

    @task
    def plan_wave(snapshot_id):
        from taldau_elt.snapshots import pending_chunk_ids
        conn=connection()
        try:
            return pending_chunk_ids(conn,snapshot_id)  # <=128 scalar IDs, never raw JSON.
        finally:
            conn.close()

    @task(pool='taldau_api',pool_slots=1,max_active_tis_per_dag=3,
          do_xcom_push=False,execution_timeout=timedelta(hours=4))
    def load_chunk(chunk_id):
        from taldau_elt.snapshots import extract_chunk
        conn=connection()
        try:
            extract_chunk(conn,chunk_id)
        finally:
            conn.close()

    @task.branch(trigger_rule='all_done',retries=0)
    def route_wave(snapshot_id,selected_ids):
        import hashlib
        from airflow.operators.python import get_current_context
        from taldau_elt.snapshots import wave_outcome
        conn=connection()
        try:
            outcome=wave_outcome(conn,snapshot_id,selected_ids)
        finally:
            conn.close()
        if outcome=='failed':
            raise ValueError('Wave failed. Inspect chunk errors and resume the SAME snapshot_id.')
        if outcome=='continue':
            ctx=get_current_context()
            next_id=snapshot_id+'__wave__'+hashlib.sha256(ctx['run_id'].encode()).hexdigest()[:24]
            ctx['ti'].xcom_push(key='next_run_id',value=next_id)
            return 'continue_snapshot'
        return 'validate_snapshot'

    @task(task_id='validate_snapshot',retries=0)
    def validate(snapshot_id):
        from taldau_elt.snapshots import validate_snapshot
        conn=connection()
        try:
            validate_snapshot(conn,snapshot_id)
            return snapshot_id
        finally:
            conn.close()

    @task(do_xcom_push=False)
    def diagnostics(snapshot_id):
        import hashlib
        import logging
        from pathlib import Path
        from taldau_elt.reports import snapshot_summary,write_summary,format_summary
        conn=connection()
        try:
            report=snapshot_summary(conn,snapshot_id)
            # Fixed safe filename; full snapshot ID is included in the report.
            name=hashlib.sha256(snapshot_id.encode()).hexdigest()[:16]
            import os
            output=Path(os.getenv('TALDAU_REPORT_DIR','/opt/airflow/data/reports'))/f'inv_snapshot_{name}_summary.json'
            write_summary(report,output)
            logging.getLogger(__name__).info('%s\nReport: %s',format_summary(report),output)
        finally:
            conn.close()

    sid=authorize_snapshot()
    discovered=discover(sid)
    ids=plan_wave(discovered)
    mapped=load_chunk.expand(chunk_id=ids)
    route=route_wave(sid,ids)
    ids >> route
    mapped >> route
    continuation=SkipExistingDagRunOperator(task_id='continue_snapshot',trigger_dag_id='taldau_inv_fixed_assets_2025',
        trigger_run_id="{{ ti.xcom_pull(task_ids='route_wave', key='next_run_id') }}",
        conf={'snapshot_id':"{{ params.snapshot_id }}",'allow_extraction':True},
        wait_for_completion=False,reset_dag_run=False)
    checked=validate(sid)
    route >> [continuation,checked]
    diagnostics(checked)  # STOP: publication is exclusively an explicit CLI action.


investments_2025()
