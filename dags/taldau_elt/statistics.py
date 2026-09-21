"""Generic metadata-driven Taldau snapshot orchestration.

Preparation is offline. HTTP is possible only from explicitly authorized discovery/chunk calls.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, RealDictCursor

from taldau_elt.loader import BronzeLoader, request_hash, tree_params

LOG = logging.getLogger(__name__)
WAVE_SIZE = 128


def _safe_id(value: str, maximum: int = 100) -> str:
    if not re.fullmatch(rf"[A-Za-z0-9_-]{{1,{maximum}}}", value):
        raise ValueError(f"Identifier must match [A-Za-z0-9_-] and be at most {maximum} characters")
    return value


def enabled_indicators(conn: Any) -> list[dict]:
    """Return only enabled rows which passed the registry's full-config constraint."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM taldau.metadata_enabled_indicators ORDER BY indicator_key")
        return [dict(row) for row in cur.fetchall()]


def read_snapshot(conn: Any, snapshot_id: str) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM taldau.bronze_snapshots WHERE snapshot_id=%s", (snapshot_id,))
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Unknown generic snapshot: {snapshot_id}")
    return dict(row)


def read_batch(conn: Any, batch_id: str) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM taldau.bronze_batches WHERE batch_id=%s", (batch_id,))
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Unknown batch: {batch_id}")
    return dict(row)


def _snapshot_name(indicator_key: str, start: int, end: int, batch_id: str) -> str:
    suffix = hashlib.sha256(batch_id.encode("utf-8")).hexdigest()[:16]
    return f"{indicator_key}-{start}-{end}-{suffix}"


def create_batch(conn: Any, batch_id: str, *, year_start: int = 2023, year_end: int = 2026,
                 snapshot_overrides: dict[str, str] | None = None,
                 resume_failed: bool = False) -> dict:
    """Create one independent frozen snapshot per configured indicator without extraction."""
    _safe_id(batch_id)
    if not 2023 <= year_start <= year_end <= 2026:
        raise ValueError("Supported batch scope is 2023..2026")
    indicators = enabled_indicators(conn)
    overrides = snapshot_overrides or {}
    snapshot_ids: list[str] = []
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("batch:" + batch_id,))
            cur.execute("SELECT year_start,year_end FROM taldau.bronze_batches WHERE batch_id=%s", (batch_id,))
            existing = cur.fetchone()
            if existing and existing != (year_start, year_end):
                raise ValueError("Batch year scope is immutable")
            if existing:
                cur.execute("SELECT indicator_key,snapshot_id FROM taldau.bronze_snapshots WHERE batch_id=%s ORDER BY indicator_key",
                            (batch_id,))
                frozen = dict(cur.fetchall())
                if not frozen:
                    raise ValueError("Existing batch has no frozen snapshots")
                if any(key not in frozen or frozen[key] != sid for key, sid in overrides.items()):
                    raise ValueError("Batch snapshot membership is immutable")
                if resume_failed:
                    cur.execute("""UPDATE taldau.bronze_snapshots
                        SET state=CASE WHEN discovery_complete THEN 'loading' ELSE 'prepared' END,last_error=NULL
                        WHERE batch_id=%s AND state='failed'""", (batch_id,))
                    cur.execute("""UPDATE taldau.bronze_chunks SET state='queued',lease_token=NULL,lease_until=NULL,last_error=NULL
                        WHERE snapshot_id IN (SELECT snapshot_id FROM taldau.bronze_snapshots WHERE batch_id=%s)
                          AND state='failed'""", (batch_id,))
                cur.execute("UPDATE taldau.bronze_batches SET state='loading',last_error=NULL WHERE batch_id=%s", (batch_id,))
                return {"batch_id": batch_id, "snapshot_ids": list(frozen.values()),
                        "indicator_count": len(frozen), "already_exists": True, "http_requests": 0}
            if not indicators:
                raise ValueError("No enabled and fully configured indicators")
            unknown = set(overrides) - {row["indicator_key"] for row in indicators}
            if unknown:
                raise ValueError(f"Snapshot override references a disabled/unknown indicator: {sorted(unknown)}")
            cur.execute("""INSERT INTO taldau.bronze_batches(batch_id,year_start,year_end)
                VALUES(%s,%s,%s) ON CONFLICT(batch_id) DO NOTHING""", (batch_id, year_start, year_end))
            for indicator in indicators:
                key = indicator["indicator_key"]
                config = indicator["source_config"]
                indicator_start = max(year_start, indicator["year_start"])
                indicator_end = min(year_end, indicator["year_end"])
                if indicator_start > indicator_end:
                    continue
                sid = overrides.get(key) or _snapshot_name(key, indicator_start, indicator_end, batch_id)
                _safe_id(sid, 160)
                cur.execute("""SELECT indicator_key,source_config,year_start,year_end,batch_id
                    FROM taldau.bronze_snapshots WHERE snapshot_id=%s FOR UPDATE""", (sid,))
                old = cur.fetchone()
                if old:
                    if old[:4] != (key, config, indicator_start, indicator_end):
                        raise ValueError(f"Frozen snapshot configuration is immutable: {sid}")
                    if old[4] not in (None, batch_id):
                        raise ValueError(f"Snapshot already belongs to another batch: {sid}")
                    cur.execute("UPDATE taldau.bronze_snapshots SET batch_id=%s WHERE snapshot_id=%s AND batch_id IS NULL",
                                (batch_id, sid))
                else:
                    pipeline_id = "statistics_" + key
                    cur.execute("""INSERT INTO taldau.metadata_elt_pipelines(pipeline_id,pipeline_type,config)
                        VALUES(%s,%s,%s) ON CONFLICT(pipeline_id) DO UPDATE SET
                        pipeline_type=excluded.pipeline_type,config=excluded.config""",
                        (pipeline_id, indicator["pipeline_type"], Json(config)))
                    discovery_run = f"{sid}:discovery"
                    cur.execute("""INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                        VALUES(%s,%s,%s,%s) ON CONFLICT(run_id) DO NOTHING""",
                        (discovery_run, pipeline_id, Json(config),
                         Json({"snapshot_id": sid, "indicator_key": key, "kind": "discovery",
                               "year_start": indicator_start, "year_end": indicator_end})))
                    cur.execute("""INSERT INTO taldau.bronze_snapshots
                        (snapshot_id,batch_id,indicator_key,source_config,year_start,year_end,discovery_run_id)
                        VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                        (sid, batch_id, key, Json(config), indicator_start, indicator_end, discovery_run))
                snapshot_ids.append(sid)
            if not snapshot_ids:
                raise ValueError("No enabled indicator overlaps the requested year scope")
            cur.execute("UPDATE taldau.bronze_batches SET state='loading',last_error=NULL WHERE batch_id=%s", (batch_id,))
    return {"batch_id": batch_id, "snapshot_ids": snapshot_ids, "indicator_count": len(snapshot_ids),
            "http_requests": 0}


def _lock(conn: Any, name: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (name,))
        if not cur.fetchone()[0]:
            conn.rollback()
            raise RuntimeError(f"Active worker owns {name}")
    conn.commit()


def _unlock(conn: Any, name: str) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (name,))
    conn.commit()


class MissingRawResponse(RuntimeError):
    pass


class GenericTrackedLoader(BronzeLoader):
    """Checkpointed conservative stream for arbitrary registry dimensions."""
    def __init__(self, *args: Any, allow_http: bool, chunk_id: int | None = None,
                 lease_token: str | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.allow_http = allow_http
        self.chunk_id = chunk_id
        self.lease_token = lease_token

    def fetch(self, terms: list[str], dimension: str, parent: str = "", depth: int = 0) -> list[dict]:
        params = tree_params(self.config, terms, dimension, parent)
        digest = request_hash(self.config["endpoint"], params)
        with self.conn:
            with self.conn.cursor() as cur:
                if self.chunk_id is not None:
                    cur.execute("""UPDATE taldau.bronze_chunks SET heartbeat_at=clock_timestamp(),
                        lease_until=clock_timestamp()+interval '15 minutes'
                        WHERE chunk_id=%s AND lease_token=%s AND state='running'""",
                        (self.chunk_id, self.lease_token))
                    if cur.rowcount != 1:
                        raise RuntimeError("Lost chunk lease")
                cur.execute("""INSERT INTO taldau.bronze_request_tasks
                    (run_id,request_hash,state,dimension,request_params)
                    VALUES(%s,%s,'queued',%s,%s) ON CONFLICT DO NOTHING""",
                    (self.run_id, digest, dimension, Json(params)))
        with self.conn.cursor() as cur:
            cur.execute("""SELECT r.id,r.response_text FROM taldau.bronze_taldau_api_raw r
                WHERE r.request_hash=%s AND r.endpoint=%s AND r.request_params=%s
                  AND r.dimension=%s AND r.tree_depth=%s
                  AND r.indicator_id=%s AND r.period_id=%s
                  AND (r.run_id=%s OR r.id=(SELECT raw_id FROM taldau.bronze_request_tasks
                       WHERE run_id=%s AND request_hash=%s AND state='complete')
                       OR r.id=(SELECT raw_id FROM taldau.bronze_reuse_raw WHERE request_hash=%s
                          AND snapshot_id=(SELECT scope->>'snapshot_id' FROM taldau.bronze_extraction_runs WHERE run_id=%s)))
                ORDER BY CASE WHEN r.run_id=%s THEN 0 ELSE 1 END LIMIT 1""",
                (digest, self.config["endpoint"], Json(params), dimension, depth,
                 self.config["indicator_id"], self.config["period_id"], self.run_id,
                 self.run_id, digest, digest, self.run_id, self.run_id))
            cached = cur.fetchone()
        if cached:
            raw_id, body = cached
            self.cache_hits += 1
            nodes = json.loads(body, parse_float=Decimal)
        else:
            if not self.allow_http:
                raise MissingRawResponse(f"Missing exact raw response {digest}; HTTP disabled")
            nodes = super().fetch(terms, dimension, parent, depth)
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM taldau.bronze_taldau_api_raw WHERE run_id=%s AND request_hash=%s",
                            (self.run_id, digest))
                raw_id = cur.fetchone()[0]
        if not isinstance(nodes, list) or any(not isinstance(n, dict) or not str(n.get("id", "")).isdigit()
                or "text" not in n or str(n.get("leaf")).lower() not in ("true", "false") for n in nodes):
            raise ValueError(f"Malformed tree response: {digest}")
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""UPDATE taldau.bronze_request_tasks SET state='complete',raw_id=%s,completed_at=now()
                    WHERE run_id=%s AND request_hash=%s""", (raw_id, self.run_id, digest))
        return nodes


def discover_snapshot(conn: Any, snapshot_id: str, *, allow_http: bool) -> dict:
    snapshot = read_snapshot(conn, snapshot_id)
    if snapshot["discovery_complete"]:
        return {"snapshot_id": snapshot_id, "chunks": snapshot["expected_chunks"], "http_requests": 0}
    run_id = snapshot["discovery_run_id"]
    _lock(conn, run_id)
    loader: GenericTrackedLoader | None = None
    try:
        config = snapshot["source_config"]
        dimensions = config["dimensions"]
        first = dimensions[0]["key"]
        terms = [str(config["roots"][item["key"]]) for item in dimensions]
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE taldau.bronze_snapshots SET state='discovering',last_error=NULL WHERE snapshot_id=%s",
                            (snapshot_id,))
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='loading',last_error=NULL WHERE run_id=%s",
                            (run_id,))
        loader = GenericTrackedLoader(conn, run_id, config, {}, allow_http=allow_http, max_requests=20000)
        for _ in loader.walk(terms, first):
            pass
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT taldau.stage_snapshot_run(%s,%s)", (snapshot_id, run_id))
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='bronze_complete',completed_at=now() WHERE run_id=%s",
                            (run_id,))
                pipeline_id = "statistics_" + snapshot["indicator_key"]
                cur.execute("""INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                    SELECT %s||':chunk:'||member_id,%s,%s,
                      jsonb_build_object('snapshot_id',%s,'indicator_key',%s,'kind','chunk',
                        'chunk_dimension',%s,'chunk_member_id',member_id,'year_start',%s,'year_end',%s)
                    FROM taldau.bronze_snapshot_members
                    WHERE snapshot_id=%s AND run_id=%s AND dimension=%s AND member_id IS NOT NULL
                    ON CONFLICT DO NOTHING""",
                    (snapshot_id, pipeline_id, Json(config), snapshot_id, snapshot["indicator_key"], first,
                     snapshot["year_start"], snapshot["year_end"], snapshot_id, run_id, first))
                cur.execute("""INSERT INTO taldau.bronze_chunks(snapshot_id,chunk_key,coordinates,run_id)
                    SELECT %s,%s||':'||member_id,jsonb_build_object(%s,member_id::text),%s||':chunk:'||member_id
                    FROM taldau.bronze_snapshot_members
                    WHERE snapshot_id=%s AND run_id=%s AND dimension=%s AND member_id IS NOT NULL
                    ON CONFLICT(snapshot_id,chunk_key) DO NOTHING""",
                    (snapshot_id, first, first, snapshot_id, snapshot_id, run_id, first))
                cur.execute("""UPDATE taldau.bronze_snapshots SET discovery_complete=true,state='loading',
                    expected_chunks=(SELECT count(*) FROM taldau.bronze_chunks WHERE snapshot_id=%s)
                    WHERE snapshot_id=%s RETURNING expected_chunks""", (snapshot_id, snapshot_id))
                chunks = cur.fetchone()[0]
        return {"snapshot_id": snapshot_id, "chunks": chunks, "http_requests": loader.http_count}
    except Exception as exc:
        conn.rollback()
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE taldau.bronze_snapshots SET state='failed',last_error=%s WHERE snapshot_id=%s",
                            (str(exc)[:4000], snapshot_id))
        raise
    finally:
        if loader:
            loader.session.close()
        _unlock(conn, run_id)


def pending_batch_chunks(conn: Any, batch_id: str, limit: int = WAVE_SIZE) -> list[int]:
    if not 1 <= limit <= WAVE_SIZE:
        raise ValueError(f"Maximum wave size is {WAVE_SIZE}")
    with conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT c.chunk_id FROM taldau.bronze_chunks c
                JOIN taldau.bronze_snapshots s USING(snapshot_id)
                WHERE s.batch_id=%s AND s.state='loading' AND c.state<>'complete'
                  AND NOT (c.state='running' AND c.lease_until>clock_timestamp())
                ORDER BY c.attempt,c.chunk_id LIMIT %s""", (batch_id, limit))
            return [row[0] for row in cur.fetchall()]


def extract_chunk(conn: Any, chunk_id: int, *, allow_http: bool) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM taldau.bronze_chunks WHERE chunk_id=%s", (chunk_id,))
        chunk = cur.fetchone()
    if not chunk:
        raise ValueError(f"Unknown chunk: {chunk_id}")
    snapshot = read_snapshot(conn, chunk["snapshot_id"])
    run_id = chunk["run_id"]
    _lock(conn, run_id)
    token = uuid.uuid4().hex
    loader: GenericTrackedLoader | None = None
    claimed = False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT state FROM taldau.bronze_chunks WHERE chunk_id=%s FOR UPDATE", (chunk_id,))
                chunk_state = cur.fetchone()[0]
                if chunk_state == "complete":
                    return {"chunk_id": chunk_id, "already_complete": True, "http_requests": 0}
                if snapshot["state"] == "failed" and chunk_state == "failed":
                    # Airflow task retry for this same durable chunk, before route_wave quarantines it.
                    cur.execute("UPDATE taldau.bronze_snapshots SET state='loading',last_error=NULL WHERE snapshot_id=%s",
                                (snapshot["snapshot_id"],))
                    snapshot["state"] = "loading"
                if snapshot["state"] != "loading":
                    raise ValueError("Snapshot must be loading")
                cur.execute("""UPDATE taldau.bronze_chunks SET state='running',attempt=attempt+1,
                    lease_token=%s,lease_until=clock_timestamp()+interval '15 minutes',heartbeat_at=clock_timestamp(),
                    started_at=coalesce(started_at,now()),last_error=NULL WHERE chunk_id=%s""", (token, chunk_id))
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='loading',last_error=NULL WHERE run_id=%s",
                            (run_id,))
        claimed = True
        config = snapshot["source_config"]
        dimensions = config["dimensions"]
        first = dimensions[0]["key"]
        terms = [str(config["roots"][item["key"]]) for item in dimensions]
        terms[0] = str(chunk["coordinates"][first])
        loader = GenericTrackedLoader(conn, run_id, config, {}, allow_http=allow_http,
                                      chunk_id=chunk_id, lease_token=token, max_requests=5000)

        def traverse(index: int, current_terms: list[str]) -> None:
            dimension = dimensions[index]["key"]
            for node in loader.walk(current_terms, dimension):
                if index + 1 < len(dimensions):
                    next_terms = list(current_terms)
                    next_terms[index] = str(node["id"])
                    traverse(index + 1, next_terms)

        if len(dimensions) > 1:
            traverse(1, terms)
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='bronze_complete',completed_at=now() WHERE run_id=%s",
                            (run_id,))
                cur.execute("SELECT taldau.stage_observation_chunk(%s)", (chunk_id,))
                staged = cur.fetchone()[0]
                cur.execute("""UPDATE taldau.bronze_chunks SET state='complete',completed_at=now(),lease_until=NULL,
                    raw_count=(SELECT count(*) FROM taldau.bronze_run_raw WHERE run_id=%s)
                    WHERE chunk_id=%s AND lease_token=%s""", (run_id, chunk_id, token))
        return {"chunk_id": chunk_id, "staged_rows": staged, "http_requests": loader.http_count,
                "cache_hits": loader.cache_hits}
    except Exception as exc:
        conn.rollback()
        if claimed:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""UPDATE taldau.bronze_chunks SET state='failed',last_error=%s,lease_until=NULL
                        WHERE chunk_id=%s AND lease_token=%s""", (str(exc)[:4000], chunk_id, token))
                    cur.execute("UPDATE taldau.bronze_snapshots SET state='failed',last_error=%s WHERE snapshot_id=%s",
                                (str(exc)[:4000], snapshot["snapshot_id"]))
                    cur.execute("UPDATE taldau.bronze_extraction_runs SET status='failed',last_error=%s WHERE run_id=%s",
                                (str(exc)[:4000], run_id))
        raise
    finally:
        if loader:
            loader.session.close()
        _unlock(conn, run_id)


def batch_wave_outcome(conn: Any, batch_id: str, selected_ids: list[int]) -> str:
    if not isinstance(selected_ids, list) or len(selected_ids) > WAVE_SIZE:
        raise ValueError("Invalid bounded wave")
    with conn:
        with conn.cursor() as cur:
            cur.execute("""UPDATE taldau.bronze_snapshots s SET state='failed',last_error='One or more chunks failed'
                WHERE batch_id=%s AND state='loading' AND EXISTS(
                  SELECT 1 FROM taldau.bronze_chunks c WHERE c.snapshot_id=s.snapshot_id AND c.state='failed')""",
                        (batch_id,))
            cur.execute("""SELECT count(*) FROM taldau.bronze_chunks c JOIN taldau.bronze_snapshots s USING(snapshot_id)
                WHERE s.batch_id=%s AND s.state='loading' AND c.state<>'complete'""", (batch_id,))
            pending = cur.fetchone()[0]
    return "continue" if pending else "validate"


def validate_batch(conn: Any, batch_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("""SELECT snapshot_id FROM taldau.bronze_snapshots
            WHERE batch_id=%s AND state NOT IN ('failed','published') ORDER BY indicator_key""", (batch_id,))
        snapshot_ids = [row[0] for row in cur.fetchall()]
    results = []
    for snapshot_id in snapshot_ids:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT taldau.validate_snapshot(%s)", (snapshot_id,))
                    results.append(cur.fetchone()[0])
        except Exception as exc:  # one indicator never rolls back another indicator's committed validation
            conn.rollback()
            LOG.exception("Validation failed for %s", snapshot_id)
            with conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE taldau.bronze_snapshots SET state='failed',last_error=%s WHERE snapshot_id=%s",
                                (str(exc)[:4000], snapshot_id))
            results.append({"snapshot_id": snapshot_id, "valid": False, "error": str(exc)})
    with conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT count(*) FILTER(WHERE state IN ('validated','published')),
                count(*) FILTER(WHERE state='failed'),count(*)
                FROM taldau.bronze_snapshots WHERE batch_id=%s""", (batch_id,))
            valid, failed, total = cur.fetchone()
            state = "validated" if valid == total else "failed" if valid == 0 else "partially_validated"
            cur.execute("UPDATE taldau.bronze_batches SET state=%s,completed_at=now() WHERE batch_id=%s", (state, batch_id))
    return {"batch_id": batch_id, "state": state, "validated": valid, "failed": failed, "results": results}


def publish_snapshot(conn: Any, snapshot_id: str) -> int:
    """Explicit action; Silver and Gold share this single transaction."""
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT taldau.publish_snapshot(%s)", (snapshot_id,))
            return cur.fetchone()[0]


def snapshot_summary(conn: Any, snapshot_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("""WITH s AS (SELECT * FROM taldau.bronze_snapshots WHERE snapshot_id=%s),
          chunks AS (SELECT c.* FROM taldau.bronze_chunks c JOIN s USING(snapshot_id)),
          cells AS (SELECT v.* FROM taldau.staging_observation_cells v JOIN s USING(snapshot_id))
        SELECT jsonb_build_object(
          'snapshot_id',s.snapshot_id,'indicator_key',s.indicator_key,'state',s.state,
          'year_start',s.year_start,'year_end',s.year_end,
          'chunks_total',(SELECT count(*) FROM chunks),
          'chunks_complete',(SELECT count(*) FROM chunks WHERE state='complete'),
          'chunks_failed',(SELECT count(*) FROM chunks WHERE state='failed'),
          'raw_responses',(SELECT count(*) FROM taldau.bronze_run_raw r WHERE r.run_id=s.discovery_run_id
             OR r.run_id IN (SELECT run_id FROM chunks)),
          'numeric_rows',(SELECT count(*) FROM cells WHERE value_status='numeric'),
          'x_rows',(SELECT count(*) FROM cells WHERE value_status='x'),
          'invalid_rows',(SELECT count(*) FROM cells WHERE value_status IN ('invalid','missing')),
          'duplicate_keys',(SELECT count(*) FROM (SELECT reporting_period,coordinates FROM cells
             WHERE value_status='numeric' GROUP BY 1,2 HAVING count(*)>1) d),
          'checks',coalesce((SELECT jsonb_agg(to_jsonb(q) ORDER BY check_name)
             FROM taldau.quality_snapshot_checks q WHERE q.snapshot_id=s.snapshot_id),'[]'::jsonb),
          'periods',coalesce((SELECT jsonb_agg(to_jsonb(p) ORDER BY period_code,reporting_period)
             FROM taldau.quality_period_diagnostics p WHERE p.snapshot_id=s.snapshot_id),'[]'::jsonb)
        ) FROM s""", (snapshot_id,))
        row = cur.fetchone()
    if not row:
        raise ValueError(f"Unknown generic snapshot: {snapshot_id}")
    return row[0]


def batch_summary(conn: Any, batch_id: str) -> dict:
    batch = read_batch(conn, batch_id)
    with conn.cursor() as cur:
        cur.execute("SELECT snapshot_id FROM taldau.bronze_snapshots WHERE batch_id=%s ORDER BY indicator_key", (batch_id,))
        snapshots = [snapshot_summary(conn, row[0]) for row in cur.fetchall()]
    return {"batch_id": batch_id, "state": batch["state"], "year_start": batch["year_start"],
            "year_end": batch["year_end"], "indicators_total": len(snapshots),
            "indicators_validated": sum(row["state"] in ("validated", "published") for row in snapshots),
            "indicators_failed": sum(row["state"] == "failed" for row in snapshots),
            "snapshots": snapshots}
