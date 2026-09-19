"""Thin SQL execution layer; business transformations live in PostgreSQL."""
from __future__ import annotations

from typing import Any


def validate_bronze(conn: Any, run_id: str) -> dict:
    with conn:
        with conn.cursor() as cur:
            cur.execute('SELECT bronze.validate_inv_pilot(%s)', (run_id,))
            return cur.fetchone()[0]


def transform_silver(conn: Any, run_id: str) -> dict:
    with conn:
        with conn.cursor() as cur:
            cur.execute('SELECT silver.refresh_inv_pilot(%s)', (run_id,))
            result = cur.fetchone()[0]
        # Same transaction: an equality failure rolls back the refreshed slice.
        result.update(_silver_checks(conn, run_id))
    return result


def _silver_checks(conn: Any, run_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute('''WITH b AS (
            SELECT kato_id,krp_id,sif_id,gsvziok_id,reporting_period_text::bigint AS reporting_period,value
            FROM bronze.v_inv_candidates WHERE run_id=%s AND value IS NOT NULL
        ), s AS (
            SELECT kato_id,krp_id,sif_id,gsvziok_id,reporting_period,value
            FROM silver.inv_fixed_assets WHERE source_run_id=%s
        ), differences AS ((SELECT * FROM b EXCEPT SELECT * FROM s)
                           UNION ALL (SELECT * FROM s EXCEPT SELECT * FROM b))
        SELECT (SELECT count(*) FROM s),(SELECT count(*) FROM differences),
          (SELECT count(*) FROM bronze.v_inv_candidates b JOIN silver.inv_fixed_assets s
             ON (s.kato_id,s.krp_id,s.sif_id,s.gsvziok_id,s.reporting_period)=
                (b.kato_id,b.krp_id,b.sif_id,b.gsvziok_id,b.reporting_period_text::bigint)
           WHERE b.run_id=%s AND b.raw_value='x' AND s.source_run_id=%s)''',
                    (run_id,run_id,run_id,run_id))
        count, differences, x_in_silver = cur.fetchone()
    if count != 504 or differences or x_in_silver:
        raise ValueError(f'Silver validation failed: count={count}, differences={differences}, x={x_in_silver}')
    return {'silver_rows': count,'bronze_silver_differences': differences,'x_in_silver': x_in_silver}


def validate_silver(conn: Any, run_id: str) -> dict:
    result = validate_bronze(conn, run_id)
    with conn:
        result.update(_silver_checks(conn, run_id))
    return result


def build_gold(conn: Any, run_id: str) -> dict:
    with conn:
        _silver_checks(conn, run_id)
        with conn.cursor() as cur:
            cur.execute('SELECT gold.refresh_inv_pilot(%s)', (run_id,))
            return {'gold_rows': cur.fetchone()[0]}


def validate_gold(conn: Any, run_id: str) -> dict:
    with conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT count(*),count(*) FILTER (WHERE g.value IS DISTINCT FROM s.value)
                FROM gold.v_inv_fixed_assets g FULL JOIN silver.inv_fixed_assets s
                ON (g.indicator_id,g.reporting_period,g.kato_id,g.krp_id,g.sif_id,g.gsvziok_id)=
                   (s.indicator_id,s.reporting_period,s.kato_id,s.krp_id,s.sif_id,s.gsvziok_id)
                WHERE g.source_run_id=%s OR s.source_run_id=%s''', (run_id,run_id))
            count,mismatches = cur.fetchone()
    if count != 504 or mismatches:
        raise ValueError(f'Gold validation failed: rows={count}, mismatches={mismatches}')
    return {'gold_rows':count,'silver_gold_mismatches':mismatches}
