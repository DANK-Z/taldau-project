"""Confirmed empty incremental scopes: synthetic Bronze, no HTTP or source DB."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch
import uuid

import psycopg2
from psycopg2.extras import Json

import test_multi_indicator_framework as framework
from taldau_elt.loader import request_hash, tree_params
from taldau_elt.statistics import (
    INCREMENTAL_OWNER, batch_summary, create_batch, discover_snapshot, extract_chunk,
    finish_incremental_batch, pending_batch_chunks, publish_batch, publish_snapshot,
    read_snapshot, validate_batch,
)

ROOT = Path(__file__).resolve().parents[1]


class NoNewPeriodsUnitTests(unittest.TestCase):
    def test_batch_publication_skips_no_new_snapshot_entirely(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ('validated',)
        cur.fetchall.return_value = [('empty', 'no_new_periods', object())]
        result = publish_batch(conn, 'batch', auto_publish=True)
        self.assertEqual(result['rows'], {})
        self.assertEqual(result['no_new_periods'], ['empty'])
        self.assertFalse(any('publish_snapshot' in call.args[0]
                             or 'staging_observation_cells' in call.args[0]
                             for call in cur.execute.call_args_list))

    def test_migration_registered_in_both_cli_tools(self):
        for name in ('manage_statistics_batch.py', 'manage_investments_snapshot.py'):
            code = (ROOT / 'tools' / name).read_text(encoding='utf-8')
            self.assertLess(code.index('012_incremental_refresh.sql'),
                            code.index('013_incremental_no_new_periods.sql'))


@unittest.skipUnless(os.getenv('TALDAU_TEST_DB') == '1', 'Requires isolated test DB')
class NoNewPeriodsDatabaseTests(unittest.TestCase):
    setUp = framework.MultiIndicatorFrameworkTests.setUp
    tearDown = framework.MultiIndicatorFrameworkTests.tearDown
    make_ready_snapshot = framework.MultiIndicatorFrameworkTests.make_ready_snapshot

    def enable_only(self, key):
        with self.conn.cursor() as cur:
            cur.execute('UPDATE taldau.metadata_indicator_registry SET enabled=(indicator_key=%s)', (key,))

    def empty_fixture(self, *, incremental=True, ready=True, nodes=None, batch_id=None):
        self.enable_only('population')
        batch_id = batch_id or 'empty_' + uuid.uuid4().hex
        kwargs = {'source_as_of': '2026-09-20', 'orchestration_key': INCREMENTAL_OWNER} if incremental else {}
        sid = create_batch(self.conn, batch_id, year_start=2026, year_end=2026, **kwargs)['snapshot_ids'][0]
        snapshot = read_snapshot(self.conn, sid)
        config = snapshot['source_config']
        terms = [str(config['roots'][d['key']]) for d in config['dimensions']]
        params = tree_params(config, terms, 'region')
        payload = nodes if nodes is not None else [
            {'id': '741880', 'text': 'Synthetic country', 'leaf': True,
             '122025': '202501', 'y122025': '10'}]
        digest = request_hash(config['endpoint'], params)
        with self.conn.cursor() as cur:
            cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                (run_id,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                 response_text,response_hash,http_status,dimension,tree_depth)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,200,'region',0) RETURNING id''',
                (snapshot['discovery_run_id'], config['indicator_id'], config['endpoint'], config['period_id'],
                 Json(params), digest, Json(payload), json.dumps(payload), uuid.uuid4().hex * 2))
            raw_id = cur.fetchone()[0]
            if ready:
                cur.execute('''INSERT INTO taldau.bronze_request_tasks
                    (run_id,request_hash,state,dimension,request_params,raw_id,completed_at)
                    VALUES(%s,%s,'complete','region',%s,%s,now())''',
                    (snapshot['discovery_run_id'], digest, Json(params), raw_id))
                cur.execute("UPDATE taldau.bronze_extraction_runs SET status='bronze_complete' WHERE run_id=%s",
                            (snapshot['discovery_run_id'],))
                cur.execute('''UPDATE taldau.bronze_snapshots SET discovery_complete=true,
                    expected_chunks=0,state='loading' WHERE snapshot_id=%s''', (sid,))
        return batch_id, sid

    def add_chunks(self, sid, states):
        with self.conn.cursor() as cur:
            for i, state in enumerate(states):
                run = sid + ':chunk:' + str(i)
                cur.execute('''INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope,status)
                    SELECT %s,'statistics_population',source_config,'{}','bronze_complete'
                    FROM taldau.bronze_snapshots WHERE snapshot_id=%s''', (run, sid))
                cur.execute('''INSERT INTO taldau.bronze_chunks(snapshot_id,chunk_key,run_id,state,attempt)
                    VALUES(%s,%s,%s,%s,1)''', (sid, str(i), run, state))
            cur.execute('UPDATE taldau.bronze_snapshots SET expected_chunks=%s WHERE snapshot_id=%s',
                        (len(states), sid))

    def assert_failed(self, batch, sid):
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'failed')
        self.assertEqual(read_snapshot(self.conn, sid)['state'], 'failed')

    def test_incremental_zero_periods_and_early_gate(self):
        batch, sid = self.empty_fixture(ready=False)
        result = discover_snapshot(self.conn, sid, allow_http=False)
        self.assertEqual(result['state'], 'no_new_periods')
        self.assertEqual(result['chunks'], 0)
        self.assertEqual(pending_batch_chunks(self.conn, batch), [])
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM taldau.bronze_extraction_runs WHERE run_id LIKE %s", (sid + ':chunk:%',))
            self.assertEqual(cur.fetchone()[0], 0)
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'validated')
        report = batch_summary(self.conn, batch)
        self.assertEqual(report['indicators_no_new_periods'], 1)
        self.assertEqual(report['indicators_failed'], 0)
        self.assertEqual(report['snapshots'][0]['available_periods'], 0)
        self.assertEqual(report['snapshots'][0]['staged_rows'], 0)
        finish_incremental_batch(self.conn, batch)
        with self.conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM taldau.metadata_batch_owners WHERE batch_id=%s', (batch,))
            self.assertEqual(cur.fetchone()[0], 0)

    def test_historical_empty_is_still_failed(self):
        batch, sid = self.empty_fixture(incremental=False, ready=False)
        self.assertEqual(discover_snapshot(self.conn, sid, allow_http=False)['chunks'], 1)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_chunks SET state='complete' WHERE snapshot_id=%s", (sid,))
        self.assert_failed(batch, sid)
        with self.conn.cursor() as cur:
            cur.execute('''SELECT check_name,violations FROM taldau.quality_snapshot_checks
                WHERE snapshot_id=%s AND severity='blocking' AND violations>0''', (sid,))
            self.assertEqual(cur.fetchall(), [('empty_snapshot', 1)])

    def test_available_period_with_zero_staging_is_failed(self):
        batch, sid = self.empty_fixture(nodes=[{'id': '741880', 'text': 'Fixture', 'leaf': True,
                                               '122026': '202601', 'y122026': '10'}])
        self.assert_failed(batch, sid)

    def test_failed_and_incomplete_chunks_never_become_no_new_periods(self):
        for state in ('failed', 'queued', 'running'):
            with self.subTest(state=state), self.conn:
                batch, sid = self.empty_fixture()
                self.add_chunks(sid, ['complete', state])
                self.assert_failed(batch, sid)
                with self.conn.cursor() as cur:
                    cur.execute('DELETE FROM taldau.metadata_batch_owners WHERE batch_id=%s', (batch,))

    def test_discovery_and_raw_errors_fail_closed(self):
        mutations = [
            "UPDATE taldau.bronze_snapshots SET discovery_complete=false WHERE snapshot_id=%s",
            "UPDATE taldau.bronze_snapshots SET expected_chunks=1 WHERE snapshot_id=%s",
            "UPDATE taldau.bronze_extraction_runs SET status='failed' WHERE run_id=(SELECT discovery_run_id FROM taldau.bronze_snapshots WHERE snapshot_id=%s)",
            "UPDATE taldau.bronze_request_tasks SET state='queued',raw_id=NULL,completed_at=NULL WHERE run_id=(SELECT discovery_run_id FROM taldau.bronze_snapshots WHERE snapshot_id=%s)",
            "DELETE FROM taldau.bronze_request_tasks WHERE run_id=(SELECT discovery_run_id FROM taldau.bronze_snapshots WHERE snapshot_id=%s)",
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                batch, sid = self.empty_fixture()
                with self.conn.cursor() as cur:
                    cur.execute(mutation, (sid,))
                self.assert_failed(batch, sid)
                with self.conn.cursor() as cur:
                    cur.execute('DELETE FROM taldau.metadata_batch_owners WHERE batch_id=%s', (batch,))

    def test_malformed_empty_or_orphan_responses_are_errors(self):
        for payload in ({'error': 'upstream'}, [], [7], [{'id': '741880', 'text': '', 'leaf': True}],
                        [{'id': '741880', 'text': 'Fixture', 'leaf': False}],
                        [{'id': '741880', 'text': 'Fixture', 'leaf': True, 'y122026': '10'}]):
            with self.subTest(payload=payload):
                batch, sid = self.empty_fixture(nodes=payload)
                self.assert_failed(batch, sid)
                # Even invalid raw must remain diagnosable.
                self.assertEqual(batch_summary(self.conn, batch)['indicators_failed'], 1)
                with self.conn.cursor() as cur:
                    cur.execute('DELETE FROM taldau.metadata_batch_owners WHERE batch_id=%s', (batch,))

    def test_http_failure_in_discovery_is_not_no_new_periods(self):
        batch, sid = self.empty_fixture(ready=False)
        with patch('taldau_elt.statistics.GenericTrackedLoader.walk', side_effect=RuntimeError('HTTP 503')):
            with self.assertRaisesRegex(RuntimeError, 'HTTP 503'):
                discover_snapshot(self.conn, sid, allow_http=False)
        self.assertFalse(read_snapshot(self.conn, sid)['discovery_complete'])
        self.assert_failed(batch, sid)

    def test_resume_incident_keeps_all_266_complete_chunks(self):
        batch, sid = self.empty_fixture(batch_id='taldau-inc-20260920T070000-6e4610389f4b44482e25856c')
        self.assertEqual(sid, 'population-2026-2026-8c670a44b35fe312')
        self.add_chunks(sid, ['complete'] * 266)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_snapshots SET state='failed' WHERE snapshot_id=%s", (sid,))
            cur.execute("UPDATE taldau.bronze_batches SET state='partially_validated' WHERE batch_id=%s", (batch,))
            cur.execute('SELECT chunk_id,attempt FROM taldau.bronze_chunks WHERE snapshot_id=%s ORDER BY chunk_id', (sid,))
            before = cur.fetchall()
        with patch('taldau_elt.statistics.GenericTrackedLoader', side_effect=AssertionError('No loader on resume')):
            create_batch(self.conn, batch, year_start=2026, year_end=2026, resume_failed=True,
                         source_as_of='2026-09-26', orchestration_key=INCREMENTAL_OWNER)
            self.assertEqual(discover_snapshot(self.conn, sid, allow_http=False)['http_requests'], 0)
            self.assertEqual(pending_batch_chunks(self.conn, batch), [])
            self.assertEqual(validate_batch(self.conn, batch)['no_new_periods'], 1)
            self.assertTrue(extract_chunk(self.conn, before[0][0], allow_http=False)['already_complete'])
        with self.conn.cursor() as cur:
            cur.execute('SELECT chunk_id,attempt FROM taldau.bronze_chunks WHERE snapshot_id=%s ORDER BY chunk_id', (sid,))
            self.assertEqual(cur.fetchall(), before)
        self.assertEqual(read_snapshot(self.conn, sid)['source_config']['incremental_as_of'], '2026-09-20')
        finish_incremental_batch(self.conn, batch)

    def test_no_new_periods_preserves_existing_silver_and_gold(self):
        self.enable_only('population')
        coords = {'region': '741880', 'locality_type': '741917', 'sex': '741935', 'population_group': '3699122'}
        _, old = self.make_ready_snapshot('population', coords, period='122026', year_start=2026, year_end=2026)
        publish_snapshot(self.conn, old)
        tables = ('silver_observations', 'gold_fact_observations', 'gold_dim_stat_indicator', 'gold_dim_member', 'gold_dim_period')
        with self.conn.cursor() as cur:
            before = {}
            for table in tables:
                cur.execute('SELECT * FROM taldau.' + table + " WHERE indicator_key='population'")
                before[table] = cur.fetchall()
            cur.execute('''CREATE FUNCTION pg_temp.reject_population_write() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN IF coalesce(NEW.indicator_key,OLD.indicator_key)='population' THEN
                  RAISE EXCEPTION 'Unexpected population write'; END IF; RETURN NEW; END $$''')
            for table in tables:
                cur.execute('CREATE TRIGGER reject_population BEFORE INSERT OR UPDATE OR DELETE ON taldau.' + table
                            + ' FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_population_write()')
        batch, sid = self.empty_fixture()
        # Direct SQL publication must also short-circuit if validation first classifies it.
        self.assertEqual(publish_snapshot(self.conn, sid), 0)
        validate_batch(self.conn, batch)
        self.assertEqual(publish_batch(self.conn, batch, auto_publish=True)['rows'], {})
        self.assertEqual(publish_snapshot(self.conn, sid), 0)
        self.assertEqual(read_snapshot(self.conn, sid)['state'], 'no_new_periods')
        with self.conn.cursor() as cur:
            for table in tables:
                cur.execute('SELECT * FROM taldau.' + table + " WHERE indicator_key='population'")
                self.assertEqual(cur.fetchall(), before[table])

    def test_mixed_seven_validated_one_no_new_periods_and_owner_release(self):
        batch, sid = self.empty_fixture()
        with self.conn.cursor() as cur:
            cur.execute("SELECT indicator_key,roots FROM taldau.metadata_indicator_registry WHERE indicator_key<>'population'")
            indicators = cur.fetchall()
        self.assertEqual(len(indicators), 7)
        for key, roots in indicators:
            self.enable_only(key)
            _, ready = self.make_ready_snapshot(key, roots, period='062026', year_start=2026, year_end=2026)
            with self.conn.cursor() as cur:
                cur.execute('UPDATE taldau.bronze_snapshots SET batch_id=%s WHERE snapshot_id=%s', (batch, ready))
        result = validate_batch(self.conn, batch)
        self.assertEqual((result['state'], result['validated'], result['no_new_periods'], result['failed']),
                         ('validated', 7, 1, 0))
        with self.conn.cursor() as cur:
            cur.execute('''CREATE FUNCTION pg_temp.reject_trade() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN IF NEW.indicator_key='trade' THEN RAISE EXCEPTION 'Synthetic trade failure';
                END IF; RETURN NEW; END $$''')
            cur.execute('''CREATE TRIGGER reject_trade BEFORE INSERT ON taldau.gold_fact_observations
                FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_trade()''')
        with self.assertRaisesRegex(psycopg2.Error, 'Synthetic trade failure'):
            publish_batch(self.conn, batch, auto_publish=True)
        self.assertEqual(read_snapshot(self.conn, sid)['state'], 'no_new_periods')
        with self.conn.cursor() as cur:
            for table in ('silver_observations', 'gold_fact_observations'):
                cur.execute('SELECT count(*) FROM taldau.' + table + ''' WHERE source_snapshot_id IN
                    (SELECT snapshot_id FROM taldau.bronze_snapshots WHERE batch_id=%s)''', (batch,))
                self.assertEqual(cur.fetchone()[0], 0)
            cur.execute('DROP TRIGGER reject_trade ON taldau.gold_fact_observations')
        published = publish_batch(self.conn, batch, auto_publish=True)
        self.assertEqual(len(published['rows']), 7)
        self.assertNotIn(sid, published['rows'])
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'published')
        finish_incremental_batch(self.conn, batch)
        with self.conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM taldau.metadata_batch_owners WHERE batch_id=%s', (batch,))
            self.assertEqual(cur.fetchone()[0], 0)

    def test_no_new_state_migration_replay_and_owner_lock(self):
        batch, sid = self.empty_fixture()
        validate_batch(self.conn, batch)
        before = read_snapshot(self.conn, sid)
        migration = (ROOT / 'dags/taldau_elt/sql/013_incremental_no_new_periods.sql').read_text(encoding='utf-8')
        with self.conn.cursor() as cur:
            cur.execute(migration)
            cur.execute(migration)
        self.assertEqual(read_snapshot(self.conn, sid), before)
        with self.assertRaisesRegex(RuntimeError, 'must finish'):
            create_batch(self.conn, 'competing_batch', year_start=2026, year_end=2026,
                         source_as_of='2026-09-20', orchestration_key=INCREMENTAL_OWNER)
        other = framework.db_connection()
        try:
            with other.cursor() as cur:
                cur.execute('SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0))', (INCREMENTAL_OWNER,))
                self.assertFalse(cur.fetchone()[0])
        finally:
            other.rollback()
            other.close()


if __name__ == '__main__':
    unittest.main()
