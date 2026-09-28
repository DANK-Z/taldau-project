"""Summary parity, bounded SQL and optional isolated EXPLAIN benchmark."""
import json
import os
from pathlib import Path
import unittest
import uuid
from unittest.mock import MagicMock

from psycopg2.extras import Json

import test_multi_indicator_framework as framework
import test_incremental_no_new_periods as no_new
from taldau_elt.loader import request_hash, tree_params
from taldau_elt.statistics import (
    _SUMMARY_SQL, batch_state_summary, batch_summary, read_snapshot, snapshot_summary, validate_batch,
)

BASELINE = (Path(__file__).parent / 'fixtures/legacy_snapshot_summary.sql').read_text(encoding='utf-8')
SNAPSHOT_KEYS = set('snapshot_id indicator_key state year_start year_end no_new_periods staged_rows '
                    'available_periods chunks_total chunks_complete chunks_failed raw_responses '
                    'numeric_rows x_rows invalid_rows duplicate_keys checks periods'.split())
BATCH_KEYS = set('batch_id state year_start year_end indicators_total indicators_validated '
                 'indicators_failed indicators_no_new_periods snapshots'.split())


class RecordingConnection:
    def __init__(self, conn):
        self.conn = conn
        self.sql = []

    def cursor(self, *args, **kwargs):
        owner = self
        class Cursor:
            def __enter__(self):
                self.cur = owner.conn.cursor(*args, **kwargs)
                return self

            def __exit__(self, *exc):
                self.cur.close()

            def execute(self, sql, params=None):
                owner.sql.append(sql)
                return self.cur.execute(sql, params)

            def __getattr__(self, name):
                return getattr(self.cur, name)
        return Cursor()


class SummaryUnitTests(unittest.TestCase):
    def test_successful_summary_is_one_aggregate_without_period_view(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [({'snapshot_id': 'fixture', 'state': 'validated'}, None)]
        snapshot_summary(conn, 'fixture')
        cur.execute.assert_called_once()
        sql = cur.execute.call_args.args[0]
        self.assertNotIn('bronze_snapshot_available_periods', sql)
        self.assertNotIn('jsonb_array_elements', sql)
        self.assertEqual(sql.count('taldau.staging_observation_cells'), 1)
        self.assertEqual(sql.count('taldau.bronze_chunks'), 1)
        self.assertIn('count(*) FILTER(WHERE numeric>1)', sql)
        self.assertNotIn('SELECT count(*) FROM cells', sql)

    def test_unknown_snapshot_keeps_cli_error(self):
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value.fetchall.return_value = []
        with self.assertRaisesRegex(ValueError, 'Unknown generic snapshot'):
            snapshot_summary(conn, 'missing')


@unittest.skipUnless(os.getenv('TALDAU_TEST_DB') == '1', 'Requires isolated test DB')
class SummaryDatabaseTests(unittest.TestCase):
    setUp = framework.MultiIndicatorFrameworkTests.setUp
    tearDown = framework.MultiIndicatorFrameworkTests.tearDown
    make_ready_snapshot = framework.MultiIndicatorFrameworkTests.make_ready_snapshot
    enable_only = no_new.NoNewPeriodsDatabaseTests.enable_only
    empty_fixture = no_new.NoNewPeriodsDatabaseTests.empty_fixture

    def source_period(self, sid, period='062026'):
        snapshot = read_snapshot(self.conn, sid)
        config = snapshot['source_config']
        first = config['dimensions'][0]['key']
        params = tree_params(config, [config['roots'][d['key']] for d in config['dimensions']], first)
        payload = [{'id': config['roots'][first], 'text': 'Fixture', 'leaf': True,
                    period: '1069', 'y' + period: '10'}]
        digest = request_hash(config['endpoint'], params)
        with self.conn.cursor() as cur:
            cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                (run_id,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                 response_text,response_hash,http_status,dimension,tree_depth)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,200,%s,0) RETURNING id''',
                (snapshot['discovery_run_id'], config['indicator_id'], config['endpoint'], config['period_id'],
                 Json(params), digest, Json(payload), json.dumps(payload), uuid.uuid4().hex * 2, first))
            raw_id = cur.fetchone()[0]
            cur.execute('''INSERT INTO taldau.bronze_request_tasks
                (run_id,request_hash,state,dimension,request_params,raw_id,completed_at)
                VALUES(%s,%s,'complete',%s,%s,%s,now())''',
                (snapshot['discovery_run_id'], digest, first, Json(params), raw_id))
            cur.execute("UPDATE taldau.bronze_extraction_runs SET status='bronze_complete' WHERE run_id=%s",
                        (snapshot['discovery_run_id'],))

    def ready(self):
        self.enable_only('grp')
        batch, sid = self.make_ready_snapshot('grp', {'region': '741880'},
                                              period='062026', year_start=2026, year_end=2026)
        self.source_period(sid)
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'validated')
        return batch, sid

    def baseline(self, sid):
        with self.conn.cursor() as cur:
            cur.execute(BASELINE, (sid,))
            return cur.fetchone()[0]

    def test_json_parity_and_query_count_including_statuses_and_duplicates(self):
        batch, sid = self.ready()
        self.assertEqual(snapshot_summary(self.conn, sid), self.baseline(sid))
        with self.conn.cursor() as cur:
            # Mixed statuses at one grain: only numeric rows contribute to duplicates.
            cur.execute('''INSERT INTO taldau.staging_observation_cells
                SELECT snapshot_id,chunk_id,run_id,raw_id,node_ordinal+i,indicator_key,indicator_id,period_code,
                  reporting_period,reporting_period_text,coordinates,coordinate_hash,has_value,has_period,
                  raw_value,value,CASE i WHEN 1 THEN 'numeric' WHEN 2 THEN 'x'
                    WHEN 3 THEN 'invalid' ELSE 'missing' END,value_measure
                FROM taldau.staging_observation_cells CROSS JOIN generate_series(1,4) i WHERE snapshot_id=%s''', (sid,))
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'failed')
        recorded = RecordingConnection(self.conn)
        report = batch_summary(recorded, batch)
        self.assertEqual(set(report), BATCH_KEYS)
        row = report['snapshots'][0]
        self.assertEqual(set(row), SNAPSHOT_KEYS)
        self.assertEqual(row, self.baseline(sid))
        self.assertEqual([row[k] for k in ('staged_rows', 'numeric_rows', 'x_rows', 'invalid_rows', 'duplicate_keys')],
                         [5, 2, 1, 2, 1])
        self.assertEqual(len(recorded.sql), 2)
        self.assertFalse(any('bronze_snapshot_available_periods' in sql for sql in recorded.sql))

    def test_mixed_batch_and_light_summary(self):
        batch, empty = self.empty_fixture()
        with self.conn.cursor() as cur:
            cur.execute("SELECT indicator_key,roots FROM taldau.metadata_indicator_registry WHERE indicator_key<>'population'")
            indicators = cur.fetchall()
        for key, roots in indicators:
            self.enable_only(key)
            _, sid = self.make_ready_snapshot(key, roots, period='062026', year_start=2026, year_end=2026)
            self.source_period(sid)
            with self.conn.cursor() as cur:
                cur.execute('UPDATE taldau.bronze_snapshots SET batch_id=%s WHERE snapshot_id=%s', (batch, sid))
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'validated')
        recorded = RecordingConnection(self.conn)
        report = batch_summary(recorded, batch)
        self.assertEqual(len(recorded.sql), 2)
        self.assertFalse(any('bronze_snapshot_available_periods' in sql for sql in recorded.sql))
        self.assertEqual((report['indicators_validated'], report['indicators_no_new_periods']), (7, 1))
        for row in report['snapshots']:
            self.assertEqual(row, self.baseline(row['snapshot_id']))
            self.assertEqual(row['available_periods'], 0 if row['snapshot_id'] == empty else 1)
        recorded.sql.clear()
        light = batch_state_summary(recorded, batch)
        self.assertEqual({k: v for k, v in light.items() if k != 'snapshots'},
                         {k: v for k, v in report.items() if k != 'snapshots'})
        self.assertFalse(any(token in sql for sql in recorded.sql
                             for token in ('staging_', 'bronze_run_raw', 'bronze_chunks', 'quality_', 'available_periods')))

    def test_historical_empty_and_failed_available_periods_are_exact(self):
        batch, sid = self.ready()
        with self.conn.cursor() as cur:
            cur.execute('DELETE FROM taldau.staging_observation_cells WHERE snapshot_id=%s', (sid,))
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'failed')
        recorded = RecordingConnection(self.conn)
        report = snapshot_summary(recorded, sid)
        self.assertEqual((report['available_periods'], report['staged_rows'], report['state']), (1, 0, 'failed'))
        self.assertEqual(report, self.baseline(sid))
        self.assertEqual(len(recorded.sql), 1)

    def test_unvalidated_cli_retains_live_inventory(self):
        batch, sid = self.empty_fixture(nodes=[{'id': '741880', 'text': 'Fixture', 'leaf': True,
                                               '122026': '1069', 'y122026': '10'}])
        self.assertEqual(snapshot_summary(self.conn, sid), self.baseline(sid))
        self.assertEqual(batch_summary(self.conn, batch)['snapshots'][0]['available_periods'], 1)

    def test_early_gate_failure_also_caches_inventory(self):
        from taldau_elt.statistics import discover_snapshot
        batch, sid = self.empty_fixture(ready=False, nodes=[
            {'id': '741880', 'text': 'Fixture', 'leaf': True, 'y122026': '10'}])
        self.assertEqual(discover_snapshot(self.conn, sid, allow_http=False)['state'], 'failed')
        recorded = RecordingConnection(self.conn)
        self.assertEqual(snapshot_summary(recorded, sid), self.baseline(sid))
        self.assertEqual(len(recorded.sql), 1)

    @unittest.skipUnless(os.getenv('TALDAU_BENCHMARK') == '1', 'Opt-in isolated EXPLAIN benchmark')
    def test_explain_benchmark(self):
        _, sid = self.ready()
        with self.conn.cursor() as cur:
            cur.execute("SET LOCAL work_mem='4MB'")
            cur.execute('''INSERT INTO taldau.staging_observation_cells
                SELECT snapshot_id,chunk_id,run_id,raw_id,node_ordinal+i,indicator_key,indicator_id,period_code,
                  reporting_period,reporting_period_text,jsonb_build_object('region',(i/5)::text),
                  md5(i::text),has_value,has_period,repeat('1',256),value,value_status,value_measure
                FROM taldau.staging_observation_cells CROSS JOIN generate_series(1,100000) i WHERE snapshot_id=%s''', (sid,))
            cur.execute('ANALYZE taldau.staging_observation_cells')
            self.assertEqual(snapshot_summary(self.conn, sid), self.baseline(sid))
            plans = {}
            for name, sql in [('legacy', BASELINE), ('aggregate', _SUMMARY_SQL.format(scope='snapshot_id'))]:
                cur.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + sql, (sid,))
                plan = cur.fetchone()[0][0]
                def nodes(node):
                    yield node
                    for child in node.get('Plans', []):
                        yield from nodes(child)
                scans = [node for node in nodes(plan['Plan'])
                         if node.get('Relation Name') == 'staging_observation_cells']
                plans[name] = {'execution_ms': plan['Execution Time'],
                               'temp_read_blocks': plan['Plan'].get('Temp Read Blocks', 0),
                               'temp_written_blocks': plan['Plan'].get('Temp Written Blocks', 0),
                               'staging_scan_nodes': len(scans),
                               'cells_cte_scans': sum(node.get('CTE Name') == 'cells' for node in nodes(plan['Plan']))}
                self.assertEqual(len(scans), 1)
            print('SUMMARY_BENCHMARK ' + json.dumps({'rows': 100001, 'work_mem': '4MB', 'plans': plans}), flush=True)
            # Timings are evidence, not a flaky CI pass/fail threshold.


if __name__ == '__main__':
    unittest.main()
