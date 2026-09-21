"""Offline Bronze audits and traversal plans. This module never constructs an HTTP client."""
from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Any

from taldau_elt.loader import request_hash, tree_params
from taldau_elt.snapshots import read_snapshot, discover_territories, extract_chunk, pending_chunk_ids


def candidate_sql(prepared: bool = False) -> str:
    """Works before migration 008 for auditing an existing source snapshot."""
    reuse = 'UNION SELECT raw_id FROM taldau.bronze_inv_reuse_raw WHERE snapshot_id=%s' if prepared else ''
    return '''WITH runs AS (
        SELECT discovery_run_id AS run_id FROM taldau.bronze_inv_snapshots WHERE snapshot_id=%s
        UNION ALL SELECT run_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s
    ), ids AS (
        SELECT id FROM taldau.bronze_taldau_api_raw WHERE run_id IN (SELECT run_id FROM runs)
        UNION SELECT raw_id FROM taldau.bronze_inv_request_tasks WHERE run_id IN (SELECT run_id FROM runs)
            AND state='complete'
        '''+reuse+'''
    ) SELECT r.* FROM taldau.bronze_taldau_api_raw r JOIN ids ON ids.id=r.id WHERE r.indicator_id=701827'''


def audit_bronze(conn: Any, snapshot_id: str, year_start: int = 2023, year_end: int = 2026,
                 *, prepared: bool = False) -> dict:
    if not 2023 <= year_start <= year_end <= 2026:
        raise ValueError('Supported years: 2023..2026')
    read_snapshot(conn,snapshot_id)
    params=[snapshot_id,snapshot_id]+([snapshot_id] if prepared else [])+[year_start,year_end]
    with conn.cursor() as cur:
        cur.execute('''WITH raw AS MATERIALIZED ('''+candidate_sql(prepared)+'''), cells AS MATERIALIZED (
            SELECT r.id,r.loaded_at,k.code,n.node ? ('y'||k.code) AND n.node ? k.code AS paired,
                n.node->>k.code AS reporting_period,n.node->>('y'||k.code) AS raw_value
            FROM raw r CROSS JOIN LATERAL jsonb_array_elements(r.response_data) n(node)
            CROSS JOIN LATERAL (SELECT DISTINCT regexp_replace(key,'^y','') AS code
                FROM jsonb_object_keys(n.node) key WHERE key ~ '^y?[0-9]{6}$'
                  AND taldau.bronze_inv_int(right(key,4)) BETWEEN %s AND %s) k
            WHERE r.dimension='gsvziok'
        ) SELECT code,reporting_period,count(*) FILTER(WHERE paired),
            count(*) FILTER(WHERE paired AND raw_value ~ '^[+-]?[0-9]+([.][0-9]+)?$'),
            count(*) FILTER(WHERE paired AND raw_value='x'),
            count(*) FILTER(WHERE NOT paired OR code !~ '^(0[1-9]|1[0-2])[0-9]{4}$'
                OR coalesce(reporting_period,'') !~ '^[0-9]{1,18}$'
                OR (coalesce(raw_value,'') !~ '^[+-]?[0-9]+([.][0-9]+)?$' AND raw_value IS DISTINCT FROM 'x')),
            count(DISTINCT id),min(loaded_at),max(loaded_at)
            FROM cells GROUP BY code,reporting_period ORDER BY right(code,4),left(code,2),reporting_period''',params)
        periods=[]
        for code,period,paired,numeric,x,invalid,raw_count,first,last in cur.fetchall():
            periods.append(dict(year=int(code[-4:]),period_code=code,reporting_period=period,
                present_in_bronze=paired>0,numeric_cells=numeric,x_cells=x,invalid_cells=invalid,
                source_raw_responses=raw_count,source_first_loaded_at=first.isoformat(),source_last_loaded_at=last.isoformat()))
        # Response counts per year must be DISTINCT across months, not sums of month counts.
        cur.execute('''WITH raw AS ('''+candidate_sql(prepared)+''')
            SELECT right(key,4)::int,count(DISTINCT r.id) FROM raw r
            CROSS JOIN LATERAL jsonb_array_elements(r.response_data) n(node)
            CROSS JOIN LATERAL jsonb_object_keys(n.node) key
            WHERE r.dimension='gsvziok' AND key ~ '^y?[0-9]{6}$'
              AND taldau.bronze_inv_int(right(key,4)) BETWEEN %s AND %s GROUP BY 1''',params)
        raw_per_year=dict(cur.fetchall())
    years=[]
    for year in range(year_start,year_end+1):
        rows=[p for p in periods if p['year']==year]
        codes=sorted({p['period_code'] for p in rows if p['present_in_bronze'] and 1<=int(p['period_code'][:2])<=12})
        years.append(dict(year=year,months_present=len(codes),period_codes=codes,
            available_through=codes[-1] if codes else None,
            numeric_cells=sum(p['numeric_cells'] for p in rows),x_cells=sum(p['x_cells'] for p in rows),
            invalid_cells=sum(p['invalid_cells'] for p in rows),source_raw_responses=raw_per_year.get(year,0),
            warning=('historical_year_has_fewer_than_12_months' if year<date.today().year else 'partial_year')
                if len(codes)<12 else None))
    mappings=Counter(p['period_code'] for p in periods)
    return dict(source_snapshot_id=snapshot_id,year_start=year_start,year_end=year_end,
        scope='gsvziok raw cells; observations, not deduplicated facts',years=years,periods=periods,
        conflicting_period_codes=[code for code,n in mappings.items() if n>1],http_requests=0)


def coverage_plan(conn: Any, snapshot_id: str, *, source_only: bool = False) -> dict:
    snapshot=read_snapshot(conn,snapshot_id)
    config=snapshot['config']
    prepared=not source_only
    params=[snapshot_id,snapshot_id]+([snapshot_id] if prepared else [])
    cache={}
    with conn.cursor() as cur:
        # Only traversal fields enter Python; facts stay in SQL/JSONB. No per-request round trips.
        cur.execute('''SELECT id,request_hash,endpoint,request_params,dimension,tree_depth,period_id,
            (SELECT coalesce(jsonb_agg(jsonb_build_object('id',n->>'id','text',n->>'text','leaf',n->>'leaf')),'[]')
             FROM jsonb_array_elements(r.response_data) n)
            FROM ('''+candidate_sql(prepared)+''') r''',params)
        for raw_id,digest,endpoint,request_params,dimension,depth,period_id,nodes in cur:
            entry=(raw_id,endpoint,request_params,dimension,depth,period_id,nodes)
            if digest in cache and cache[digest][1:]!=entry[1:]:
                raise ValueError('Ambiguous cached traversal versions; use a frozen source snapshot')
            cache[digest]=entry
    missing={}; reused={}

    def walk(terms,dimension):
        seen=set()

        def visit(parent='',depth=0):
            if depth>30: raise ValueError('Tree depth exceeded in cached source')
            params=tree_params(config,terms,dimension,parent)
            digest=request_hash(config['endpoint'],params)
            entry=cache.get(digest)
            if entry is None or entry[1:6]!=(config['endpoint'],params,dimension,depth,config['period_id']):
                missing[digest]=dict(request_hash=digest,dimension=dimension,request_params=params)
                return
            reused[digest]=entry[0]
            for node in entry[6]:
                node_id=node['id']
                if not str(node_id).isdigit() or node_id in seen or str(node['leaf']).lower() not in ('true','false'):
                    raise ValueError('Invalid/cyclic cached tree; inspect raw before extraction')
                seen.add(node_id)
                yield node
                if str(node['leaf']).lower()=='false':
                    yield from visit(node_id,depth+1)
        yield from visit()

    roots=config['roots']; terms=[roots[d] for d in ('kato','krp','sif','gsvziok')]
    territories=list(walk(terms,'kato'))
    discovery_missing=len(missing)
    chunks=[]
    for territory in territories:
        before=set(missing); hits=set(reused)
        terms=[territory['id'],roots['krp'],roots['sif'],roots['gsvziok']]
        for krp in walk(terms,'krp'):
            for sif in walk([terms[0],krp['id'],roots['sif'],roots['gsvziok']],'sif'):
                for _ in walk([terms[0],krp['id'],sif['id'],roots['gsvziok']],'gsvziok'): pass
        chunks.append(dict(territory_id=int(territory['id']),reusable_requests=len(set(reused)-hits),
            missing_requests=len(set(missing)-before),fully_reusable=len(missing)==len(before)))
    return dict(snapshot_id=snapshot_id,reusable_requests=len(reused),missing_requests=len(missing),
        reusable_raw_responses=len(set(reused.values())),expected_http_requests=0 if not missing else None,
        expected_http_requests_lower_bound=len(missing),estimate_is_exact=not missing,
        unknown_descendants=bool(missing),discovery_complete=discovery_missing==0,
        territories=len(territories),chunks_fully_reusable=sum(c['fully_reusable'] for c in chunks),
        chunks=chunks,missing=list(missing.values()),http_requests=0,
        note='Missing frontier requests may reveal further requests. Cached months do not prove current source freshness.')


def reuse_offline(conn: Any, snapshot_id: str) -> dict:
    """Rebuild control plane/staging using references only; never publish or fetch HTTP."""
    snapshot=read_snapshot(conn,snapshot_id)
    if snapshot['state'] in ('validated','published'):
        return dict(snapshot_id=snapshot_id,already_complete=True,http_requests=0)
    plan=coverage_plan(conn,snapshot_id)
    if not plan['estimate_is_exact']:
        return plan  # Keep an incomplete snapshot untouched; explicit launch can fill the frontier.
    discover_territories(conn,snapshot_id,allow_http=False)
    while ids:=pending_chunk_ids(conn,snapshot_id):
        for chunk_id in ids:
            extract_chunk(conn,chunk_id,allow_http=False)
    return dict(snapshot_id=snapshot_id,chunks_complete=plan['territories'],http_requests=0,
                next_step='validate, diagnostics, then separately approved manual publish')
