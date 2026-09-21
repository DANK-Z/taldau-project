"""Compare direct ELT with the existing local reference CSV, never label it a live ETS check."""
import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

from run_investments_elt import ROOT
from taldau_elt.pipeline import validate_silver, validate_gold


def verify(conn, run_id, reference):
    report = validate_silver(conn,run_id)
    report['validated_at_utc'] = datetime.now(timezone.utc).isoformat()
    report.update(validate_gold(conn,run_id))
    with reference.open(encoding='utf-8-sig',newline='') as f:
        rows = list(csv.DictReader(f))
    with conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TEMP TABLE pilot_reference(
                krp_id bigint,sif_id bigint,gsvziok_id bigint,reporting_period bigint,value numeric
            ) ON COMMIT DROP''')
            execute_values(cur,'INSERT INTO pilot_reference VALUES %s',[
                (int(r['krp_id']),int(r['sif_id']),int(r['gsvziok_id']),int(r['reporting_period']),Decimal(r['value']))
                for r in rows])
            cur.execute('''SELECT count(*) FROM (SELECT krp_id,sif_id,gsvziok_id,reporting_period
                FROM pilot_reference GROUP BY 1,2,3,4 HAVING count(*)>1) d''')
            report['reference_duplicates'] = cur.fetchone()[0]
            cur.execute('''WITH s AS (SELECT * FROM taldau.silver_inv_fixed_assets WHERE source_run_id=%s)
                SELECT count(*) FILTER(WHERE s.krp_id IS NULL),
                       count(*) FILTER(WHERE r.krp_id IS NULL),
                       count(*) FILTER(WHERE s.krp_id IS NOT NULL AND r.krp_id IS NOT NULL AND s.value<>r.value),
                       count(*) FILTER(WHERE s.value=r.value)
                FROM pilot_reference r FULL JOIN s USING(krp_id,sif_id,gsvziok_id,reporting_period)''',(run_id,))
            missing,extra,mismatch,matched = cur.fetchone()
            report.update(reference_missing=missing,reference_extra=extra,reference_value_mismatch=mismatch,reference_matched=matched)
            cur.execute('''SELECT value FROM taldau.silver_inv_fixed_assets
                WHERE source_run_id=%s AND kato_id=268012 AND krp_id=741927
                AND sif_id=807855 AND gsvziok_id=19202537 AND reporting_period=1069''',(run_id,))
            report['control_value'] = str(cur.fetchone()[0])
            cur.execute('SELECT count(*),sum(octet_length(response_text)),count(DISTINCT request_hash) FROM taldau.bronze_taldau_api_raw WHERE run_id=%s',(run_id,))
            count, size, unique = cur.fetchone()
            report.update(bronze_responses=count,bronze_response_bytes=size,bronze_unique_requests=unique)
            cur.execute("SELECT to_regclass('public.bns_inv_fixed_assets') IS NOT NULL")
            report['local_ets_table_available'] = cur.fetchone()[0]
    report['reference_path'] = str(reference)
    report['reference_sha256'] = hashlib.sha256(reference.read_bytes()).hexdigest()
    if missing or extra or mismatch or matched!=504 or report['reference_duplicates'] or report['control_value']!='1798175695000':
        raise ValueError(json.dumps(report,ensure_ascii=False,default=str))
    return report


if __name__=='__main__':
    import os
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',default='astana-2025-12-pilot-v1')
    parser.add_argument('--output',type=Path,default=ROOT/'data'/'reports'/'astana_elt_validation.json')
    args=parser.parse_args()
    conn=psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=os.getenv('PGPORT','5434'),
                          dbname=os.getenv('PGDATABASE','taldau'),user=os.getenv('PGUSER','taldau'),
                          password=os.getenv('PGPASSWORD'))
    try:
        report=verify(conn,args.run_id,ROOT/'data'/'astana_2025_12_taldau.csv')
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
        print(json.dumps(report,ensure_ascii=False,default=str))
    finally:
        conn.close()
