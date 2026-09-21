"""HTTP -> immutable raw Bronze. Python only interprets tree control fields."""
from __future__ import annotations

import hashlib
import json
import logging
import time
from decimal import Decimal
from typing import Any, Iterator

import requests
from psycopg2.extras import Json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOG = logging.getLogger(__name__)
PILOT_SCOPE = {"kato_id": "268012", "period_code": "122025", "reporting_period": 1069,
               "expected_numeric": 504, "expected_x": 7}


def request_hash(endpoint: str, params: dict[str, str]) -> str:
    text = json.dumps([endpoint, params], sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def tree_params(config: dict, terms: list[str], dimension: str, parent: str = '') -> dict[str, str]:
    """One canonical parameter builder shared by pilot and snapshot checkpoints."""
    return {"p_measure_id": str(config['measure_id']), "p_index_id": str(config['indicator_id']),
            "p_period_id": str(config['period_id']), "p_terms": ','.join(map(str, terms)),
            "p_term_id": str(config['roots'][dimension]), "p_dicIds": config['dic_ids'],
            "idx": str(config['idx']), "p_parent_id": str(parent)}


def get_config(conn: Any) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT config FROM taldau.metadata_elt_pipelines WHERE pipeline_id=%s AND pipeline_type='cube'",
                    ('inv_fixed_assets_monthly',))
        row = cur.fetchone()
    if not row:
        raise ValueError('Run Bronze migration first')
    return row[0]


class BronzeLoader:
    def __init__(self, conn: Any, run_id: str, config: dict, scope: dict,
                 *, delay: float = 0.2, max_requests: int = 1000):
        self.conn, self.run_id, self.config, self.scope = conn, run_id, config, scope
        self.delay, self.max_requests = delay, max_requests
        self.http_count = self.cache_hits = 0
        self.session = requests.Session()
        retry = Retry(total=5, connect=5, read=5, status=5, backoff_factor=1,
                      status_forcelist=(429, 500, 502, 503, 504), allowed_methods={'GET'},
                      respect_retry_after_header=True, raise_on_status=False)
        self.session.mount('https://', HTTPAdapter(max_retries=retry, pool_maxsize=1))

    def fetch(self, terms: list[str], dimension: str, parent: str = '', depth: int = 0) -> list[dict]:
        c = self.config
        params = tree_params(c, terms, dimension, parent)
        digest = request_hash(c['endpoint'], params)
        with self.conn.cursor() as cur:
            cur.execute('SELECT response_text FROM taldau.bronze_taldau_api_raw WHERE run_id=%s AND request_hash=%s',
                        (self.run_id, digest))
            cached = cur.fetchone()
        self.conn.commit()
        if cached:
            self.cache_hits += 1
            body = cached[0]
        else:
            if self.http_count >= self.max_requests:
                raise RuntimeError('Request budget reached; use the same run_id to resume')
            time.sleep(self.delay)
            response = self.session.get(c['endpoint'], params=params, timeout=(10, 90))
            self.http_count += 1
            LOG.info('HTTP %s dimension=%s parent=%s request=%s',
                     response.status_code, dimension, parent, self.http_count)
            response.raise_for_status()
            body = response.text
            # Validate JSON syntax without rewriting, rounding or filtering its values.
            json.loads(body, parse_float=Decimal)
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                        (run_id,indicator_id,endpoint,period_id,request_params,request_hash,
                         response_data,response_text,response_hash,http_status,dimension,tree_depth)
                        VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
                        ON CONFLICT (run_id,request_hash) DO NOTHING''',
                        (self.run_id,c['indicator_id'],c['endpoint'],c['period_id'],Json(params),digest,
                         body,body,hashlib.sha256(body.encode('utf-8')).hexdigest(),
                         response.status_code,dimension,depth))
        nodes = json.loads(body, parse_float=Decimal)
        # Structural validation happens AFTER raw storage; malformed source remains inspectable.
        if not isinstance(nodes, list):
            raise ValueError(f'Expected tree array: {dimension}, parent={parent}')
        for node in nodes:
            if not isinstance(node, dict) or not str(node.get('id', '')).isdigit() or 'text' not in node:
                raise ValueError(f'Invalid tree node in {digest}')
            if str(node.get('leaf')).lower() not in ('true', 'false'):
                raise ValueError(f'Unknown leaf flag in {digest}')
        return nodes

    def walk(self, terms: list[str], dimension: str) -> Iterator[dict]:
        seen: set[str] = set()

        def visit(parent: str, depth: int) -> Iterator[dict]:
            if depth > 30:
                raise ValueError('Tree depth exceeded')
            for node in self.fetch(terms, dimension, parent, depth):
                node_id = str(node['id'])
                if node_id in seen:
                    raise ValueError(f'Duplicate/cyclic {dimension} node: {node_id}')
                seen.add(node_id)
                yield node  # Intermediate nodes are retained, including aggregate members.
                if str(node['leaf']).lower() == 'false':
                    yield from visit(node_id, depth + 1)

        yield from visit('', 0)

    def extract_pilot(self) -> dict:
        # Deliberate guard: country-wide extraction is not enabled by this pilot.
        if self.scope != PILOT_SCOPE:
            raise ValueError('Only the approved Astana December 2025 pilot is enabled')
        with self.conn.cursor() as cur:
            cur.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0))', (self.run_id,))
            if not cur.fetchone()[0]:
                raise RuntimeError('This run is already being extracted')
        self.conn.commit()
        owns_run = False
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute('''INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                        VALUES (%s,'inv_fixed_assets_monthly',%s,%s) ON CONFLICT DO NOTHING''',
                        (self.run_id, Json(self.config), Json(self.scope)))
                    cur.execute('SELECT config,scope,status FROM taldau.bronze_extraction_runs WHERE run_id=%s', (self.run_id,))
                    config, scope, status = cur.fetchone()
                    if config != self.config or scope != self.scope:
                        raise ValueError('run_id already belongs to another configuration/scope')
                    owns_run = True
                    if status in ('bronze_complete', 'silver_validated', 'gold_validated'):
                        return {'run_id': self.run_id, 'http_requests': 0, 'already_complete': True}
                    cur.execute("UPDATE taldau.bronze_extraction_runs SET status='loading',last_error=NULL WHERE run_id=%s", (self.run_id,))
            roots = self.config['roots']
            terms = [roots[d] for d in ('kato','krp','sif','gsvziok')]
            # Preserve actual country -> Astana edge; do not walk other territories.
            territories = self.fetch(terms, 'kato')
            if not any(str(n['id']) == self.scope['kato_id'] for n in territories):
                territories = self.fetch(terms, 'kato', roots['kato'], 1)
            if not any(str(n['id']) == self.scope['kato_id'] for n in territories):
                raise ValueError('Astana not found under source country root')
            terms[0] = self.scope['kato_id']
            for krp in self.walk(terms, 'krp'):
                sif_terms = [terms[0], str(krp['id']), roots['sif'], roots['gsvziok']]
                for sif in self.walk(sif_terms, 'sif'):
                    gsv_terms = [terms[0],str(krp['id']),str(sif['id']),roots['gsvziok']]
                    for _ in self.walk(gsv_terms, 'gsvziok'):
                        pass  # No fact/value transformation in Python.
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("UPDATE taldau.bronze_extraction_runs SET status='bronze_complete',completed_at=now() WHERE run_id=%s", (self.run_id,))
            return {'run_id': self.run_id, 'http_requests': self.http_count, 'cache_hits': self.cache_hits}
        except Exception as exc:
            self.conn.rollback()
            if owns_run:
                with self.conn:
                    with self.conn.cursor() as cur:
                        cur.execute("UPDATE taldau.bronze_extraction_runs SET status='failed',last_error=%s WHERE run_id=%s AND status IN ('loading','failed')",
                                    (str(exc)[:4000], self.run_id))
            LOG.exception('Extraction failed; committed Bronze requests can be resumed')
            raise
        finally:
            self.session.close()
            with self.conn.cursor() as cur:
                cur.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))', (self.run_id,))
            self.conn.commit()
