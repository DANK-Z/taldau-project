"""Prepare/inspect snapshots offline; only 'launch --confirm-full-extraction' schedules HTTP."""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import subprocess
import shlex
import sys
import uuid

import psycopg2

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'dags'))
from taldau_elt.snapshots import create_snapshot,validate_snapshot,publish_snapshot,read_snapshot
from taldau_elt.reports import snapshot_summary,format_summary
from taldau_elt.coverage import audit_bronze,coverage_plan,reuse_offline


def db_connection():
    return psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=os.getenv('PGPORT','5434'),
        dbname=os.getenv('PGDATABASE','taldau'),user=os.getenv('PGUSER','taldau'),
        password=os.getenv('PGPASSWORD'),connect_timeout=10)


def diagnostics(conn,snapshot_id):
    return snapshot_summary(conn,snapshot_id)


def launch(snapshot_id):
    base=shlex.split(os.environ['TALDAU_AIRFLOW_COMMAND']) if os.getenv('TALDAU_AIRFLOW_COMMAND') else [
        'docker','compose','exec','-T','airflow-scheduler','airflow']
    # Check the shared pool, do not silently change its size.
    output=subprocess.run(base+['pools','list','-o','json'],cwd=ROOT,capture_output=True,text=True,check=True)
    pools=json.loads(output.stdout)
    selected=[p for p in pools if p['pool']=='taldau_api']
    if len(selected)!=1 or not 1<=int(selected[0]['slots'])<=3:
        raise RuntimeError('taldau_api must have 1..3 slots; no launch performed')
    subprocess.run(base+['dags','unpause','taldau_inv_fixed_assets_2025'],cwd=ROOT,check=True)
    conf={'snapshot_id':snapshot_id,'allow_extraction':True}
    subprocess.run(base+['dags','trigger','taldau_inv_fixed_assets_2025',
        '--run-id',snapshot_id+'__manual__'+uuid.uuid4().hex,
        '--conf',json.dumps(conf)],cwd=ROOT,check=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['migrate','prepare','status','summary','diagnostics','validate','publish','launch',
                                       'audit','coverage','plan','reuse'])
    parser.add_argument('--snapshot-id')
    parser.add_argument('--year-start',type=int)
    parser.add_argument('--year-end',type=int)
    parser.add_argument('--reuse-snapshot-id')
    parser.add_argument('--source-snapshot-id',help='Read-only audit/plan of an existing source; no new snapshot required')
    parser.add_argument('--confirm-full-extraction',action='store_true')
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if args.action!='migrate' and not args.snapshot_id and not (
            args.action in ('audit','coverage','plan') and args.source_snapshot_id):
        parser.error('--snapshot-id is required')
    if args.action=='launch' and not args.confirm_full_extraction:
        parser.error('No launch: pass --confirm-full-extraction only after approval')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    conn=db_connection()
    try:
        if args.action in ('audit','coverage','plan','status','summary','diagnostics'):
            conn.set_session(readonly=True,isolation_level='REPEATABLE READ')
        if args.action=='migrate':
            names=['004_snapshot_model.sql','005_snapshot_validation.sql','006_ets_reconciliation.sql',
                   '007_gold_snapshot_publish.sql','008_multi_year_snapshot.sql']
            with conn:
                with conn.cursor() as cur:
                    for name in names:
                        cur.execute((ROOT/'dags/taldau_elt/sql'/name).read_text(encoding='utf-8'))
            result={'migrations':names,'http_requests':0}
        elif args.action=='prepare':
            result=create_snapshot(conn,args.snapshot_id,year_start=args.year_start,year_end=args.year_end,
                                   reuse_snapshot_id=args.reuse_snapshot_id)
        elif args.action=='launch':
            # Existing ranges/reuse sources remain frozen; resume never resets them to defaults.
            try:
                snapshot=read_snapshot(conn,args.snapshot_id)
            except ValueError:
                create_snapshot(conn,args.snapshot_id,year_start=args.year_start,year_end=args.year_end,
                                reuse_snapshot_id=args.reuse_snapshot_id)
                snapshot=read_snapshot(conn,args.snapshot_id)
            if snapshot['state'] in ('validated','published'):
                raise ValueError('Snapshot already complete; extraction must not be restarted')
            launch(args.snapshot_id)
            result={'snapshot_id':args.snapshot_id,'airflow_triggered':True}
        elif args.action in ('audit','coverage','plan'):
            sid=args.source_snapshot_id or args.snapshot_id
            snapshot=read_snapshot(conn,sid)
            start=args.year_start or snapshot.get('year_start') or snapshot['year']
            end=args.year_end or snapshot.get('year_end') or start
            result=audit_bronze(conn,sid,start,end,prepared=not bool(args.source_snapshot_id))
            if args.action in ('coverage','plan'):
                result.update(coverage_plan(conn,sid,source_only=bool(args.source_snapshot_id)))
        elif args.action=='reuse':
            result=reuse_offline(conn,args.snapshot_id)
        elif args.action=='validate':
            result=validate_snapshot(conn,args.snapshot_id)
        elif args.action=='publish':
            result={'published_rows':publish_snapshot(conn,args.snapshot_id)}
        else:
            result=diagnostics(conn,args.snapshot_id)
        rendered=json.dumps(result,ensure_ascii=False,indent=2,default=str)
        if args.output:
            args.output.parent.mkdir(parents=True,exist_ok=True)
            args.output.write_text(rendered+'\n',encoding='utf-8')
        print(format_summary(result) if args.action=='summary' else rendered)
    finally:
        conn.close()


if __name__=='__main__':
    main()
