"""Explicit CLI for the bounded pilot; uses standard PG* connection variables."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'dags'))
from taldau_elt.loader import BronzeLoader, PILOT_SCOPE, get_config


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['migrate-bronze','extract','migrate-silver','transform','validate','gold'])
    parser.add_argument('--run-id', default='astana-2025-12-pilot-v1')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    conn = psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=os.getenv('PGPORT','5434'),
                            dbname=os.getenv('PGDATABASE','taldau'), user=os.getenv('PGUSER','taldau'),
                            password=os.getenv('PGPASSWORD'), connect_timeout=10)
    try:
        if args.action.startswith('migrate-'):
            files = ['001_bronze.sql'] if args.action == 'migrate-bronze' else ['002_silver.sql','003_gold.sql']
            with conn:
                with conn.cursor() as cur:
                    for name in files:
                        cur.execute((ROOT/'dags'/'taldau_elt'/'sql'/name).read_text(encoding='utf-8'))
            result = {'migrated': files}
        elif args.action == 'extract':
            result = BronzeLoader(conn, args.run_id, get_config(conn), PILOT_SCOPE).extract_pilot()
        else:
            from taldau_elt.pipeline import transform_silver, validate_silver, build_gold
            result = {'transform': transform_silver,'validate':validate_silver,'gold':build_gold}[args.action](conn,args.run_id)
        print(json.dumps(result, ensure_ascii=False, default=str))
    finally:
        conn.close()


if __name__ == '__main__':
    main()
