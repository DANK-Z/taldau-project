"""Bounded ELT pilot: Astana, December 2025. No country-wide extraction."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param


def _connection():
    import psycopg2
    from airflow.hooks.base import BaseHook

    c = BaseHook.get_connection('taldau_dwh')
    return psycopg2.connect(host=c.host,port=c.port or 5432,dbname=c.schema,
                            user=c.login,password=c.password,connect_timeout=10)


@dag(dag_id='taldau_inv_fixed_assets',schedule=None,catchup=False,max_active_runs=1,
     start_date=pendulum.datetime(2026,9,1,tz='Asia/Almaty'),
     default_args={'retries':2,'retry_delay':timedelta(minutes=1),'retry_exponential_backoff':True},
     params={'bronze_run_id':Param(None,type=['null','string'],
             description='Existing Bronze run for offline replay; null captures a new API version.')},
     tags=['taldau','elt','cube','pilot'])
def taldau_inv_fixed_assets():
    @task
    def get_metadata():
        from airflow.operators.python import get_current_context
        from taldau_elt.loader import get_config, PILOT_SCOPE

        context = get_current_context()
        run_id = context['params'].get('bronze_run_id') or context['run_id']
        conn = _connection()
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT config,scope FROM bronze.extraction_runs WHERE run_id=%s',(run_id,))
                existing = cur.fetchone()
            config,scope = existing if existing else (get_config(conn),PILOT_SCOPE)
            return {'run_id':run_id,'config':config,'scope':scope}
        finally:
            conn.close()

    @task(pool='taldau_api',pool_slots=1,execution_timeout=timedelta(hours=2))
    def extract_load_bronze(metadata):
        from taldau_elt.loader import BronzeLoader

        conn = _connection()
        try:
            return BronzeLoader(conn,metadata['run_id'],metadata['config'],metadata['scope']).extract_pilot()['run_id']
        finally:
            conn.close()

    @task
    def validate_bronze(source_run_id):
        from taldau_elt.pipeline import validate_bronze as validate
        conn = _connection()
        try:
            print(validate(conn,source_run_id))
            return source_run_id
        finally:
            conn.close()

    @task
    def transform_silver_sql(source_run_id):
        from taldau_elt.pipeline import transform_silver
        conn = _connection()
        try:
            print(transform_silver(conn,source_run_id))
            return source_run_id
        finally:
            conn.close()

    @task
    def validate_silver(source_run_id):
        from taldau_elt.pipeline import validate_silver as validate
        conn = _connection()
        try:
            print(validate(conn,source_run_id))
            return source_run_id
        finally:
            conn.close()

    @task
    def build_gold_sql(source_run_id):
        from taldau_elt.pipeline import build_gold
        conn = _connection()
        try:
            print(build_gold(conn,source_run_id))
            return source_run_id
        finally:
            conn.close()

    @task
    def validate_gold(source_run_id):
        from taldau_elt.pipeline import validate_gold as validate
        conn = _connection()
        try:
            return validate(conn,source_run_id)
        finally:
            conn.close()

    validate_gold(build_gold_sql(validate_silver(transform_silver_sql(
        validate_bronze(extract_load_bronze(get_metadata()))))))


taldau_inv_fixed_assets()
