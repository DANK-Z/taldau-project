"""Country-wide control plane. Calling create_snapshot alone NEVER performs HTTP."""
from __future__ import annotations

import json
from decimal import Decimal
import logging
import re
import uuid
from typing import Any

from psycopg2.extras import Json, RealDictCursor

from taldau_elt.loader import BronzeLoader, get_config, request_hash, tree_params

LOG = logging.getLogger(__name__)
WAVE_SIZE = 128


def read_snapshot(conn: Any, snapshot_id: str) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT * FROM taldau.bronze_inv_snapshots WHERE snapshot_id=%s', (snapshot_id,))
        row = cur.fetchone()
    if row is None:
        raise ValueError(f'Unknown snapshot: {snapshot_id}')
    return dict(row)


def create_snapshot(conn: Any, snapshot_id: str, year: int = 2025, *,
                    year_start: int | None = None, year_end: int | None = None,
                    reuse_snapshot_id: str | None = None) -> dict:
    start = year if year_start is None else year_start
    end = start if year_end is None else year_end
    if not 2023 <= start <= end <= 2026 or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', snapshot_id):
        raise ValueError('Use 2023 <= year_start <= year_end <= 2026 and a safe snapshot_id')
    with conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', ('snapshot:'+snapshot_id,))
            cur.execute('SELECT coalesce(year_start,year),coalesce(year_end,year),reuse_snapshot_id '
                        'FROM taldau.bronze_inv_snapshots WHERE snapshot_id=%s', (snapshot_id,))
            existing = cur.fetchone()
            if existing:
                if existing != (start,end,reuse_snapshot_id):
                    raise ValueError('Snapshot period scope and reuse source are immutable')
                return {'snapshot_id': snapshot_id, 'already_exists': True}
            config = get_config(conn)
            if config['indicator_id'] != 701827 or config['period_id'] != 8:
                raise ValueError('Unexpected investment cube configuration')
            if reuse_snapshot_id:
                cur.execute('SELECT config,state FROM taldau.bronze_inv_snapshots WHERE snapshot_id=%s FOR SHARE',
                            (reuse_snapshot_id,))
                source = cur.fetchone()
                if not source or source[0] != config or source[1] not in ('validated','published'):
                    raise ValueError('Reuse requires a validated/published snapshot with the exact frozen config')
            discovery_run = f'{snapshot_id}:discovery'
            scope = {'snapshot_id':snapshot_id,'year':start,'year_start':start,'year_end':end,'kind':'discovery'}
            cur.execute("""INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                VALUES (%s,'inv_fixed_assets_monthly',%s,%s)""",
                (discovery_run, Json(config), Json(scope)))
            cur.execute("""INSERT INTO taldau.bronze_inv_snapshots
                (snapshot_id,year,year_start,year_end,reuse_snapshot_id,config,discovery_run_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", (snapshot_id,start,start,end,reuse_snapshot_id,Json(config),discovery_run))
            if reuse_snapshot_id:
                cur.execute("""SELECT request_hash FROM taldau.bronze_inv_run_raw WHERE run_id IN (
                    SELECT discovery_run_id FROM taldau.bronze_inv_snapshots WHERE snapshot_id=%s
                    UNION ALL SELECT run_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s)
                    GROUP BY request_hash HAVING count(DISTINCT (endpoint,request_params,response_hash))>1 LIMIT 1""",
                    (reuse_snapshot_id,reuse_snapshot_id))
                if cur.fetchone():
                    raise ValueError('Reuse source has conflicting versions for an exact request')
                cur.execute("""INSERT INTO taldau.bronze_inv_reuse_raw(snapshot_id,request_hash,raw_id)
                    SELECT %s,request_hash,min(id) FROM taldau.bronze_inv_run_raw WHERE run_id IN (
                        SELECT discovery_run_id FROM taldau.bronze_inv_snapshots WHERE snapshot_id=%s
                        UNION ALL SELECT run_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s)
                    GROUP BY request_hash""",(snapshot_id,reuse_snapshot_id,reuse_snapshot_id))
    return {'snapshot_id':snapshot_id,'state':'prepared','year_start':start,'year_end':end,'http_requests':0}


def _lock(conn: Any, name: str) -> None:
    with conn.cursor() as cur:
        cur.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0))', (name,))
        if not cur.fetchone()[0]:
            conn.rollback()
            raise RuntimeError(f'Active worker owns {name}; do not steal a live chunk')
    conn.commit()


def _unlock(conn: Any, name: str) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))', (name,))
    conn.commit()


class MissingRawResponse(RuntimeError):
    pass


class TrackedLoader(BronzeLoader):
    """Reuses raw transport; journals request intent and refreshes the worker lease."""
    def __init__(self, *args, allow_http: bool = True, chunk_id: int | None = None, token: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.chunk_id, self.token = chunk_id, token
        self.allow_http = allow_http

    def fetch(self, terms, dimension, parent='', depth=0):
        params = tree_params(self.config,terms,dimension,parent)
        digest = request_hash(self.config['endpoint'],params)
        with self.conn:
            with self.conn.cursor() as cur:
                if self.chunk_id is not None:
                    cur.execute('''UPDATE taldau.bronze_inv_chunks SET heartbeat_at=clock_timestamp(),
                        lease_until=clock_timestamp()+interval '15 minutes'
                        WHERE chunk_id=%s AND lease_token=%s AND state='running' ''',
                        (self.chunk_id,self.token))
                    if cur.rowcount != 1:
                        raise RuntimeError('Lost chunk lease')
                cur.execute('''INSERT INTO taldau.bronze_inv_request_tasks(run_id,request_hash,state,dimension,request_params)
                    VALUES (%s,%s,'queued',%s,%s) ON CONFLICT DO NOTHING''',
                    (self.run_id,digest,dimension,Json(params)))
        # Prefer an already pinned checkpoint, then owned raw, then frozen source references.
        with self.conn.cursor() as cur:
            cur.execute("""SELECT r.id,r.response_text FROM taldau.bronze_taldau_api_raw r
                WHERE r.request_hash=%s AND r.endpoint=%s AND r.request_params=%s
                  AND r.dimension=%s AND r.tree_depth=%s AND r.indicator_id=%s AND r.period_id=%s
                  AND (r.run_id=%s OR r.id=(SELECT raw_id FROM taldau.bronze_inv_request_tasks
                       WHERE run_id=%s AND request_hash=%s AND state='complete')
                       OR r.id=(SELECT raw_id FROM taldau.bronze_inv_reuse_raw WHERE request_hash=%s
                            AND snapshot_id=(SELECT scope->>'snapshot_id' FROM taldau.bronze_extraction_runs WHERE run_id=%s)))
                ORDER BY CASE WHEN r.id=(SELECT raw_id FROM taldau.bronze_inv_request_tasks
                    WHERE run_id=%s AND request_hash=%s) THEN 0 WHEN r.run_id=%s THEN 1 ELSE 2 END LIMIT 1""",
                (digest,self.config['endpoint'],Json(params),dimension,depth,self.config['indicator_id'],self.config['period_id'],
                 self.run_id,self.run_id,digest,digest,self.run_id,self.run_id,digest,self.run_id))
            cached = cur.fetchone()
        if cached:
            raw_id,body = cached
            nodes = json.loads(body,parse_float=Decimal)
            if not isinstance(nodes,list) or any(not isinstance(n,dict) or not str(n.get('id','')).isdigit()
                    or 'text' not in n or str(n.get('leaf')).lower() not in ('true','false') for n in nodes):
                raise ValueError('Invalid cached tree response')
            self.cache_hits += 1
        else:
            if not self.allow_http:
                raise MissingRawResponse(f'Missing exact raw: {digest}; HTTP disabled')
            nodes = super().fetch(terms,dimension,parent,depth)
            with self.conn.cursor() as cur:
                cur.execute('SELECT id FROM taldau.bronze_taldau_api_raw WHERE run_id=%s AND request_hash=%s',
                            (self.run_id,digest))
                raw_id = cur.fetchone()[0]
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""UPDATE taldau.bronze_inv_request_tasks SET state='complete',raw_id=%s,completed_at=now()
                    WHERE run_id=%s AND request_hash=%s""", (raw_id,self.run_id,digest))
                if cur.rowcount != 1:
                    raise RuntimeError('Raw response missing from checkpoint')
        return nodes


def discover_territories(conn: Any, snapshot_id: str, *, allow_http: bool = True) -> dict:
    """Complete source KATO traversal, including parents. Called only by an authorized DAG run."""
    snapshot = read_snapshot(conn,snapshot_id)
    run_id = snapshot['discovery_run_id']
    _lock(conn,run_id)
    loader = None
    try:
        snapshot = read_snapshot(conn,snapshot_id)
        if snapshot['discovery_complete']:
            return {'snapshot_id':snapshot_id,'chunks':snapshot['expected_chunks'],'http_requests':0}
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE taldau.bronze_inv_snapshots SET state='discovering',last_error=NULL WHERE snapshot_id=%s", (snapshot_id,))
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='loading',last_error=NULL WHERE run_id=%s", (run_id,))
        config = snapshot['config']
        loader = TrackedLoader(conn,run_id,config,{},allow_http=allow_http,max_requests=20000)
        terms = [config['roots'][d] for d in ('kato','krp','sif','gsvziok')]
        for _ in loader.walk(terms,'kato'):
            pass
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT taldau.bronze_stage_inv_run(%s,%s)', (snapshot_id,run_id))
                cur.execute('SELECT taldau.quality_inv_run_coverage(%s)', (run_id,))
                errors = cur.fetchone()[0]
                if errors:
                    raise ValueError(f'Incomplete KATO traversal: {errors}')
                cur.execute('SELECT taldau.quality_inv_discovery_errors(%s)', (snapshot_id,))
                if cur.fetchone()[0]:
                    raise ValueError('Invalid or conflicting KATO hierarchy; inspect saved raw responses')
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='bronze_complete',completed_at=now() WHERE run_id=%s", (run_id,))
                # Chunk IDs and all metadata stay in PostgreSQL, not XCom.
                cur.execute('''INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                    SELECT %s||':kato:'||member_id,'inv_fixed_assets_monthly',%s,
                        jsonb_build_object('snapshot_id',%s,'year',%s,'year_start',%s,'year_end',%s,'kato_id',member_id,'kind','territory')
                    FROM taldau.bronze_inv_snapshot_members WHERE run_id=%s AND dimension='kato'
                    ON CONFLICT DO NOTHING''', (snapshot_id,Json(config),snapshot_id,snapshot['year'],snapshot['year_start'] or snapshot['year'],snapshot['year_end'] or snapshot['year'],run_id))
                cur.execute('''INSERT INTO taldau.bronze_inv_chunks(snapshot_id,territory_id,run_id)
                    SELECT %s,member_id,%s||':kato:'||member_id
                    FROM taldau.bronze_inv_snapshot_members WHERE run_id=%s AND dimension='kato'
                    ON CONFLICT DO NOTHING''', (snapshot_id,snapshot_id,run_id))
                cur.execute('''UPDATE taldau.bronze_inv_snapshots SET discovery_complete=true,state='loading',
                    expected_chunks=(SELECT count(*) FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s)
                    WHERE snapshot_id=%s RETURNING expected_chunks''', (snapshot_id,snapshot_id))
                chunks = cur.fetchone()[0]
        return {'snapshot_id':snapshot_id,'chunks':chunks,'http_requests':loader.http_count}
    except Exception as exc:
        conn.rollback()
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE taldau.bronze_inv_snapshots SET state='failed',last_error=%s WHERE snapshot_id=%s", (str(exc)[:4000],snapshot_id))
        raise
    finally:
        if loader:
            loader.session.close()
        _unlock(conn,run_id)


def pending_chunk_ids(conn: Any, snapshot_id: str, limit: int = WAVE_SIZE) -> list[int]:
    if not 1 <= limit <= WAVE_SIZE:
        raise ValueError(f'Maximum wave size is {WAVE_SIZE}')
    with conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT discovery_complete,state FROM taldau.bronze_inv_snapshots WHERE snapshot_id=%s FOR UPDATE''', (snapshot_id,))
            row = cur.fetchone()
            if not row or not row[0]:
                raise ValueError('Discovery must complete before mapping')
            if row[1] in ('validated','published'):
                return []
            # Fail instead of letting the workflow publish around another active worker.
            cur.execute('''SELECT count(*) FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s
                AND state='running' AND lease_until>clock_timestamp()''', (snapshot_id,))
            if cur.fetchone()[0]:
                raise RuntimeError('A chunk still has a live lease; resume after the worker finishes or the lease expires')
            cur.execute("UPDATE taldau.bronze_inv_snapshots SET state='loading',last_error=NULL WHERE snapshot_id=%s", (snapshot_id,))
            cur.execute('''SELECT chunk_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s AND state<>'complete'
                ORDER BY chunk_id LIMIT %s''', (snapshot_id,limit))
            return [r[0] for r in cur.fetchall()]


def extract_chunk(conn: Any, chunk_id: int, *, allow_http: bool = True) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT * FROM taldau.bronze_inv_chunks WHERE chunk_id=%s', (chunk_id,))
        chunk = cur.fetchone()
    if not chunk:
        raise ValueError(f'Unknown chunk: {chunk_id}')
    run_id = chunk['run_id']
    _lock(conn,run_id)
    loader = None
    token = uuid.uuid4().hex
    claimed = False
    try:
        snapshot = read_snapshot(conn,chunk['snapshot_id'])
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT state,lease_until FROM taldau.bronze_inv_chunks WHERE chunk_id=%s FOR UPDATE', (chunk_id,))
                if cur.fetchone()[0] == 'complete':
                    return {'chunk_id':chunk_id,'already_complete':True,'http_requests':0}
                if snapshot['state'] != 'loading':
                    raise ValueError('Snapshot must be loading')
                # Session advisory lock is authoritative even if a HTTP retry outlives the lease.
                cur.execute('''UPDATE taldau.bronze_inv_chunks SET state='running',attempt=attempt+1,
                    lease_token=%s,lease_until=clock_timestamp()+interval '15 minutes',
                    heartbeat_at=clock_timestamp(),started_at=coalesce(started_at,now()),last_error=NULL
                    WHERE chunk_id=%s''', (token,chunk_id))
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='loading',last_error=NULL WHERE run_id=%s", (run_id,))
        claimed = True
        config = snapshot['config']
        roots = config['roots']
        loader = TrackedLoader(conn,run_id,config,{},allow_http=allow_http,chunk_id=chunk_id,token=token,max_requests=5000)
        terms = [str(chunk['territory_id']),roots['krp'],roots['sif'],roots['gsvziok']]
        for krp in loader.walk(terms,'krp'):
            sif_terms = [terms[0],str(krp['id']),roots['sif'],roots['gsvziok']]
            for sif in loader.walk(sif_terms,'sif'):
                gsv_terms = [terms[0],str(krp['id']),str(sif['id']),roots['gsvziok']]
                for _ in loader.walk(gsv_terms,'gsvziok'):
                    pass
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT taldau.quality_inv_run_coverage(%s)', (run_id,))
                if cur.fetchone()[0]:
                    raise ValueError('Request manifest or traversal coverage is incomplete')
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='bronze_complete',completed_at=now() WHERE run_id=%s", (run_id,))
                cur.execute('SELECT taldau.staging_stage_inv_chunk(%s)', (chunk_id,))
                staged = cur.fetchone()[0]
                cur.execute('''UPDATE taldau.bronze_inv_chunks SET state='complete',completed_at=now(),lease_until=NULL,
                    raw_count=(SELECT count(*) FROM taldau.bronze_inv_run_raw WHERE run_id=%s)
                    WHERE chunk_id=%s AND lease_token=%s''', (run_id,chunk_id,token))
        LOG.info('Completed chunk=%s staged_cells=%s http=%s cache_hits=%s',chunk_id,staged,loader.http_count,loader.cache_hits)
        return {'chunk_id':chunk_id,'http_requests':loader.http_count,'cache_hits':loader.cache_hits}
    except Exception as exc:
        conn.rollback()
        if claimed:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('''UPDATE taldau.bronze_inv_chunks SET state='failed',last_error=%s,lease_until=NULL
                        WHERE chunk_id=%s AND lease_token=%s''', (str(exc)[:4000],chunk_id,token))
                    cur.execute("UPDATE taldau.bronze_extraction_runs SET status='failed',last_error=%s WHERE run_id=%s", (str(exc)[:4000],run_id))
        LOG.exception('Chunk %s failed; raw responses are preserved for resume',chunk_id)
        raise
    finally:
        if loader:
            loader.session.close()
        _unlock(conn,run_id)


def wave_outcome(conn: Any, snapshot_id: str, selected_ids: list[int]) -> str:
    if not isinstance(selected_ids,list) or len(selected_ids)>WAVE_SIZE:
        raise ValueError('Missing or invalid wave plan; refusing automatic continuation')
    with conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT state,count(*) FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s GROUP BY state''', (snapshot_id,))
            counts = dict(cur.fetchall())
            cur.execute('''SELECT count(*) FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s
                AND chunk_id=ANY(%s) AND state='complete' ''', (snapshot_id,selected_ids))
            selected_complete=cur.fetchone()[0]
            if counts.get('failed',0) or counts.get('running',0) or selected_complete!=len(selected_ids):
                cur.execute("UPDATE taldau.bronze_inv_snapshots SET state='failed',last_error=%s WHERE snapshot_id=%s",
                            (f'Unfinished wave: {counts}',snapshot_id))
                result = 'failed'
            else:
                result = 'continue' if counts.get('queued',0) else 'validate'
    return result


def validate_snapshot(conn: Any, snapshot_id: str) -> dict:
    # Store diagnostic failures in a committed transaction; raise only afterwards.
    with conn:
        with conn.cursor() as cur:
            cur.execute('SELECT taldau.quality_validate_inv_snapshot(%s)', (snapshot_id,))
            result = cur.fetchone()[0]
    if not result['valid']:
        raise ValueError(f'Snapshot validation failed: {result}; see taldau.quality_inv_snapshot_checks')
    return result


def publish_snapshot(conn: Any, snapshot_id: str) -> int:
    """Publish Silver and Gold atomically; any failure rolls back both layers."""
    with conn:
        with conn.cursor() as cur:
            cur.execute('SELECT taldau.silver_publish_inv_snapshot(%s)', (snapshot_id,))
            silver_rows = cur.fetchone()[0]
            cur.execute('SELECT taldau.gold_publish_inv_snapshot(%s)', (snapshot_id,))
            gold_rows = cur.fetchone()[0]
            if gold_rows != silver_rows:
                raise ValueError(f'Gold/Silver publication count mismatch: {gold_rows} != {silver_rows}')
    return silver_rows
