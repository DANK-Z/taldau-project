"""Read-only snapshot summary shared by the DAG and the manual CLI."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def snapshot_summary(conn: Any, snapshot_id: str) -> dict:
    # One SQL statement gives counts, checks and tables one consistent MVCC view,
    # including when the operator requests progress while chunks are still running.
    with conn.cursor() as cur:
        cur.execute('''WITH s AS (
            SELECT * FROM bronze.inv_snapshots WHERE snapshot_id=%s
        ), chunks AS MATERIALIZED (
            SELECT c.* FROM bronze.inv_chunks c JOIN s USING(snapshot_id)
        ), cells AS MATERIALIZED (
            SELECT v.*,
                ((value IS NULL AND raw_value IS DISTINCT FROM 'x')
                  OR NOT has_value OR NOT has_period OR indicator_id IS NULL OR reporting_period IS NULL
                  OR kato_id IS NULL OR krp_id IS NULL OR sif_id IS NULL OR gsvziok_id IS NULL
                  OR (period_code !~ '^(0[1-9]|1[0-2])[0-9]{4}$' OR bronze.inv_int(right(period_code,4)) NOT BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year))
                  OR kato_id IS DISTINCT FROM c.territory_id
                  OR indicator_id IS DISTINCT FROM (s.config->>'indicator_id')::bigint
                  OR v.run_id<>c.run_id) AS invalid
            FROM staging.inv_year_cells v JOIN s USING(snapshot_id) JOIN chunks c USING(chunk_id)
        ), duplicates AS (
            SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,count(*) AS n
            FROM cells GROUP BY 1,2,3,4,5,6 HAVING count(*)>1
        ), actual_months AS (
            SELECT period_code,coalesce(reporting_period,-1) AS reporting_period,count(value) AS numeric_rows,
                count(*) FILTER(WHERE raw_value='x') AS x_rows,count(*) FILTER(WHERE invalid) AS invalid_rows
            FROM cells GROUP BY 1,2
        ), months AS (
            SELECT bronze.inv_int(right(coalesce(a.period_code,e.period_code),4)) AS year,
                a.period_code IS NOT NULL AS present_in_snapshot,
                coalesce(a.period_code,e.period_code) AS period_code,
                coalesce(a.reporting_period,e.reporting_period) AS reporting_period,
                coalesce(a.numeric_rows,0) AS numeric_rows,coalesce(a.x_rows,0) AS x_rows,
                coalesce(a.invalid_rows,0) AS invalid_rows,e.expected_rows AS expected_ets_rows,
                coalesce(a.numeric_rows,0)-e.expected_rows AS delta
            FROM actual_months a FULL JOIN (
                SELECT e.* FROM quality.inv_month_expectations e,s WHERE e.year BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year)
            ) e ON a.period_code=e.period_code AND a.reporting_period=e.reporting_period
        ), checks AS (
            SELECT q.* FROM quality.inv_snapshot_checks q JOIN s USING(snapshot_id)
        ), empty_chunks AS (
            SELECT c.chunk_id,c.territory_id,c.run_id,c.state,c.raw_count,c.last_error,
                count(v.raw_id) AS staged_cells,count(v.value) AS numeric_rows,
                count(*) FILTER(WHERE v.raw_value='x') AS x_rows
            FROM chunks c LEFT JOIN cells v USING(chunk_id)
            GROUP BY c.chunk_id,c.territory_id,c.run_id,c.state,c.raw_count,c.last_error
            HAVING count(v.value)=0
        )
        SELECT jsonb_build_object(
            'snapshot_id',s.snapshot_id,'snapshot_state',s.state,'year',s.year,
            'year_start',coalesce(s.year_start,s.year),'year_end',coalesce(s.year_end,s.year),
            'reuse_snapshot_id',s.reuse_snapshot_id,
            'years',(SELECT jsonb_agg(to_jsonb(y) ORDER BY year) FROM quality.inv_year_coverage y WHERE y.snapshot_id=s.snapshot_id),
            'generated_at',statement_timestamp(),'validated_at',s.validated_at,'published_at',s.published_at,
            'ready_to_publish',s.state='validated' AND s.validated_at IS NOT NULL
                AND s.discovery_complete AND (SELECT count(*) FROM chunks)=s.expected_chunks
                AND NOT EXISTS(SELECT 1 FROM chunks WHERE state<>'complete')
                AND EXISTS(SELECT 1 FROM checks) AND NOT EXISTS(SELECT 1 FROM checks WHERE violations>0),
            'territories_total',(SELECT count(DISTINCT member_id) FROM bronze.inv_snapshot_members
                WHERE run_id=s.discovery_run_id AND dimension='kato'),
            'chunks_total',(SELECT count(*) FROM chunks),
            'chunks_complete',(SELECT count(*) FROM chunks WHERE state='complete'),
            'chunks_failed',(SELECT count(*) FROM chunks WHERE state='failed'),
            'chunks_running',(SELECT count(*) FROM chunks WHERE state='running'),
            'chunks_queued',(SELECT count(*) FROM chunks WHERE state='queued'),
            'raw_responses',(SELECT count(DISTINCT id) FROM bronze.inv_run_raw WHERE run_id=s.discovery_run_id
                OR run_id IN (SELECT run_id FROM chunks)),
            'numeric_rows',(SELECT count(value) FROM cells),
            'x_rows',(SELECT count(*) FROM cells WHERE raw_value='x'),
            'invalid_rows',(SELECT count(*) FROM cells WHERE invalid),
            'duplicate_keys',(SELECT count(*) FROM duplicates),
            'duplicate_rows',(SELECT coalesce(sum(n-1),0) FROM duplicates),
            'expected_ets_rows',(SELECT sum(expected_rows) FROM quality.inv_month_expectations WHERE year BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year)),
            'delta',(SELECT sum(delta) FROM months),
            'blocking_checks_run',(SELECT count(*) FROM checks),
            'blocking_checks_failed',(SELECT count(*) FROM checks WHERE violations>0),
            'months',coalesce((SELECT jsonb_agg(to_jsonb(m) ORDER BY year,period_code,reporting_period) FROM months m),'[]'::jsonb),
            'checks',coalesce((SELECT jsonb_agg(to_jsonb(q) ORDER BY check_name) FROM checks q),'[]'::jsonb),
            'chunks_without_facts',coalesce((SELECT jsonb_agg(to_jsonb(e) ORDER BY territory_id) FROM empty_chunks e),'[]'::jsonb),
            'last_error',s.last_error
        ) FROM s''', (snapshot_id,))
        row = cur.fetchone()
    if row is None:
        raise ValueError(f'Unknown snapshot: {snapshot_id}')
    return row[0]


def write_summary(report: dict, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str)+'\n', encoding='utf-8')


def format_summary(report: dict) -> str:
    lines = [f'{key}: {report[key]}' for key in (
        'snapshot_id','snapshot_state','year_start','year_end','ready_to_publish','territories_total','chunks_total',
        'chunks_complete','chunks_failed','chunks_running','chunks_queued','raw_responses',
        'numeric_rows','x_rows','invalid_rows','duplicate_keys','expected_ets_rows','delta',
        'blocking_checks_run','blocking_checks_failed')]
    columns = ('year','period_code','reporting_period','numeric_rows','x_rows','invalid_rows','expected_ets_rows','delta')
    lines += ['', ' | '.join(columns)]
    lines += [' | '.join(str(row.get(key)) for key in columns) for row in report['months']]
    lines += ['', 'Observed year coverage:']
    lines += [f"{y['year']}: months={y['months_present']} available_through={y['available_through']} warning={y['warning']}" for y in report['years']]
    lines += ['', 'Blocking quality checks:']
    lines += [f"{row['check_name']}: {row['violations']}" for row in report['checks']]
    lines += ['', 'Chunks without numeric facts (unfinished chunks are not yet evidence of missing source data):']
    lines += [f"chunk={row['chunk_id']} territory={row['territory_id']} state={row['state']} "
              f"cells={row['staged_cells']} x={row['x_rows']}" for row in report['chunks_without_facts']]
    return '\n'.join(lines)
