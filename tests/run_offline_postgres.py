"""Run DB tests in disposable PostgreSQL using a read-only copy of cached pilot Bronze.

Requires the local postgres:17 image and the pilot fixture in the source PG* database.
Never runs extraction or writes to the source database; no persistent Docker volume.
"""
from __future__ import annotations

import os
import secrets
from pathlib import Path
import subprocess
import sys
import time
import uuid

import psycopg2

ROOT=Path(__file__).resolve().parents[1]
PILOT='astana-2025-12-pilot-v1'


def main() -> int:
    source=psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=os.getenv('PGPORT','5434'),
        dbname=os.getenv('PGDATABASE','taldau'),user=os.getenv('PGUSER','taldau'),
        password=os.getenv('PGPASSWORD'),connect_timeout=10)
    source.set_session(readonly=True)
    try:
        with source.cursor() as cur:
            cur.execute('''SELECT run_id,pipeline_id,config::text,scope::text FROM bronze.extraction_runs
                WHERE run_id=%s''',(PILOT,))
            run=cur.fetchone()
            if run is None:
                raise RuntimeError('Cached pilot fixture is required; no HTTP fallback')
            cur.execute('''SELECT run_id,indicator_id,endpoint,period_id,request_params::text,request_hash,
                response_data::text,response_text,response_hash,http_status,dimension,tree_depth
                FROM bronze.taldau_api_raw WHERE run_id=%s ORDER BY id''',(PILOT,))
            raw=cur.fetchall()
    finally:
        source.close()
    name='taldau-offline-tests-'+uuid.uuid4().hex[:12]
    password=secrets.token_urlsafe(32)
    container_id=None
    try:
        container_id=subprocess.check_output(['docker','run','--detach','--rm','--pull','never',
            '--name',name,'--tmpfs','/var/lib/postgresql/data',
            '--publish','127.0.0.1::5432','--env','POSTGRES_DB=taldau',
            '--env','POSTGRES_USER=taldau','--env','POSTGRES_PASSWORD','postgres:17'],
            env={**os.environ,'POSTGRES_PASSWORD':password},text=True).strip()
        port=subprocess.check_output(['docker','port',container_id,'5432/tcp'],text=True).strip().rsplit(':',1)[1]
        config=dict(host='127.0.0.1',port=port,dbname='taldau',user='taldau',password=password,connect_timeout=2)
        deadline=time.monotonic()+60
        while True:
            try:
                probe=psycopg2.connect(**config)
                probe.close()
                break
            except psycopg2.OperationalError:
                if time.monotonic()>=deadline: raise
                time.sleep(0.5)
        env=os.environ.copy()
        env.update(PGHOST=config['host'],PGPORT=port,PGDATABASE='taldau',PGUSER='taldau',
                   PGPASSWORD=password,TALDAU_TEST_DB='1',PYTHONIOENCODING='utf-8')
        # Exercise the existing deployment commands, including safe migration replay.
        commands=[['tools/run_investments_elt.py','migrate-bronze'],
                  ['tools/run_investments_elt.py','migrate-silver'],
                  ['tools/manage_investments_snapshot.py','migrate'],
                  ['tools/manage_investments_snapshot.py','migrate']]
        for command in commands:
            subprocess.run([sys.executable]+command,cwd=ROOT,env=env,check=True)
        target=psycopg2.connect(**config)
        try:
            with target:
                with target.cursor() as cur:
                    cur.execute('''INSERT INTO bronze.extraction_runs(run_id,pipeline_id,config,scope,status)
                        VALUES(%s,%s,%s::jsonb,%s::jsonb,'bronze_complete')''',run)
                    cur.executemany('''INSERT INTO bronze.taldau_api_raw
                        (run_id,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                         response_text,response_hash,http_status,dimension,tree_depth)
                        VALUES(%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s,%s,%s,%s)''',raw)
                    cur.execute('SELECT silver.refresh_inv_pilot(%s)',(PILOT,))
                    cur.execute('SELECT gold.refresh_inv_pilot(%s)',(PILOT,))
        finally:
            target.close()
        print(f'Isolated test database ready; copied {len(raw)} cached responses; HTTP extraction disabled.',flush=True)
        selectors=sys.argv[1:]
        arguments=selectors+['-v'] if selectors else ['discover','-s','tests','-v']
        return subprocess.run([sys.executable,'-m','unittest']+arguments,
            cwd=ROOT/'tests' if selectors else ROOT,env=env).returncode
    finally:
        if container_id:
            # Stop only the exact ephemeral container created by this invocation.
            subprocess.run(['docker','stop',container_id],check=True,stdout=subprocess.DEVNULL)


if __name__=='__main__':
    raise SystemExit(main())
