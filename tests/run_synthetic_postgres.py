"""Run generic SQL tests in a disposable PostgreSQL 17 database, with synthetic raw only.

No source database, .env, production fixture, persistent volume or external API is used.
Requires a preinstalled postgres:17 Docker image and psycopg2 in this Python runtime.
"""
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import time
import uuid

import psycopg2

ROOT = Path(__file__).resolve().parents[1]


def main():
    name = 'taldau-synthetic-tests-' + uuid.uuid4().hex[:12]
    # Empty Docker config: do not load the user's registry credentials.
    with tempfile.TemporaryDirectory(prefix='taldau-test-docker-') as config:
        docker = ['docker', '--config', config]
        container = None
        try:
            container = subprocess.check_output(docker + [
                'run', '--detach', '--rm', '--pull', 'never', '--name', name,
                '--tmpfs', '/var/lib/postgresql/data', '--publish', '127.0.0.1::5432',
                '--env', 'POSTGRES_DB=taldau_test_incremental', '--env', 'POSTGRES_USER=taldau_test',
                '--env', 'POSTGRES_HOST_AUTH_METHOD=trust', 'postgres:17'], text=True).strip()
            port = subprocess.check_output(docker + ['port', container, '5432/tcp'], text=True).strip().rsplit(':', 1)[1]
            db = dict(host='127.0.0.1', port=port, dbname='taldau_test_incremental',
                      user='taldau_test', password='synthetic-only', connect_timeout=2)
            deadline = time.monotonic() + 45
            while True:
                try:
                    conn = psycopg2.connect(**db)
                    break
                except psycopg2.OperationalError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.5)
            try:
                with conn:
                    with conn.cursor() as cur:
                        for migration in sorted((ROOT / 'dags/taldau_elt/sql').glob('*.sql')):
                            cur.execute(migration.read_text(encoding='utf-8'))
                        # A foreign-key anchor for synthetic observation fixtures. No pilot cache.
                        cur.execute('''INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                            SELECT 'synthetic-anchor',pipeline_id,config,'{}'
                            FROM taldau.metadata_elt_pipelines WHERE pipeline_id='statistics_investments_fixed_assets' ''')
                        cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                            (run_id,indicator_id,endpoint,period_id,request_params,request_hash,
                             response_data,response_text,response_hash,http_status,dimension,tree_depth)
                            VALUES('synthetic-anchor',701827,'https://invalid.test/offline',8,'{}',
                             repeat('a',64),'[]','[]',repeat('b',64),200,'kato',0)''')
            finally:
                conn.close()
            env = {**os.environ, 'PGHOST': db['host'], 'PGPORT': port, 'PGDATABASE': db['dbname'],
                   'PGUSER': db['user'], 'PGPASSWORD': db['password'],
                   'TALDAU_TEST_DB': '1', 'TALDAU_LEGACY_TEST_DATABASE': '', 'PYTHONIOENCODING': 'utf-8'}
            env.pop('PGSERVICE', None)
            env.pop('PGSERVICEFILE', None)
            selectors = sys.argv[1:] or ['test_multi_indicator_framework', 'test_incremental_statistics',
                                        'test_single_taldau_schema.FreshSingleSchemaTests']
            print('Isolated TALDAU_TEST_DB: disposable PostgreSQL 17, synthetic fixtures only.', flush=True)
            return subprocess.run([sys.executable, '-m', 'unittest', *selectors, '-v'],
                                  cwd=ROOT / 'tests', env=env).returncode
        finally:
            if container:
                subprocess.run(docker + ['stop', container], check=True, stdout=subprocess.DEVNULL)


if __name__ == '__main__':
    raise SystemExit(main())
