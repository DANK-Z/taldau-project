"""Incremental contracts; SQL checks run only in an explicitly isolated test DB."""
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import unittest
import uuid
from unittest.mock import MagicMock, patch

import psycopg2
from psycopg2.extras import Json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'dags'))
from taldau_elt.statistics import (
    INCREMENTAL_OWNER, create_batch, discover_snapshot, extract_chunk,
    finish_incremental_batch, incremental_request, publish_batch, read_snapshot,
    validate_batch, validate_year_scope,
)
import test_multi_indicator_framework as framework


class IncrementalUnitTests(unittest.TestCase):
    def test_logical_year_timezone_and_stable_unique_batch(self):
        logical = datetime(2026, 12, 31, 20, tzinfo=timezone.utc)
        first = incremental_request({}, logical, 'scheduled__2027:01:01+05:00')
        self.assertEqual((first['year_start'], first['year_end']), (2027, 2027))
        self.assertEqual(first['source_as_of'], '2027-01-01')
        self.assertRegex(first['batch_id'], r'^[A-Za-z0-9_-]{1,100}$')
        self.assertEqual(first, incremental_request({}, logical, 'scheduled__2027:01:01+05:00'))
        self.assertNotEqual(first['batch_id'], incremental_request({}, logical, 'manual__other')['batch_id'])

    def test_explicit_backfill_and_future_years(self):
        for end in (2027, 2028, 2035, 2100):
            result = incremental_request({'year_start': 2023, 'year_end': end},
                datetime(2027, 3, 20, tzinfo=timezone.utc), 'manual')
            self.assertEqual((result['year_start'], result['year_end']), (2023, end))

    def test_invalid_scope_fails_before_io(self):
        for start, end in ((2028, 2027), (2022, 2027), (2023, 2101), (True, 2027), ('2027', 2027)):
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                validate_year_scope(start, end)
        with self.assertRaisesRegex(ValueError, 'both'):
            incremental_request({'year_end': 2027}, datetime.now(timezone.utc), 'manual')
        with self.assertRaisesRegex(ValueError, 'original batch_id'):
            incremental_request({'resume_failed': True}, datetime.now(timezone.utc), 'manual')

    def test_auto_publish_default_performs_no_sql(self):
        conn = MagicMock()
        self.assertFalse(publish_batch(conn, 'fixture')['published'])
        conn.cursor.assert_not_called()

    def test_auto_publish_blocks_batch_and_quality_violations(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ('partially_validated',)
        with self.assertRaisesRegex(ValueError, 'validation'):
            publish_batch(conn, 'fixture', auto_publish=True)
        cur.fetchall.return_value = [('fixture-snapshot', 'validated', datetime.now(timezone.utc))]
        for checks, violations, invalid in ((20, 1, 0), (20, 0, 1), (0, 0, 0)):
            cur.reset_mock()
            cur.fetchone.side_effect = [('validated',), (checks, violations), (invalid,)]
            with self.assertRaisesRegex(ValueError, 'quality'):
                publish_batch(conn, 'fixture', auto_publish=True)
            self.assertFalse(any('SELECT taldau.publish_snapshot' in call.args[0]
                                 for call in cur.execute.call_args_list))

    def test_lookup_index_and_incremental_migration_are_registered(self):
        for name in ('010_multi_indicator_framework.sql', '012_incremental_refresh.sql'):
            sql = (ROOT / 'dags/taldau_elt/sql' / name).read_text(encoding='utf-8')
            self.assertIn('CREATE INDEX IF NOT EXISTS gold_fact_observations_lookup_idx', sql)
            self.assertIn('(indicator_key, coordinates, reporting_period) INCLUDE (value)', sql)
        for name in ('manage_statistics_batch.py', 'manage_investments_snapshot.py'):
            code = (ROOT / 'tools' / name).read_text(encoding='utf-8')
            self.assertLess(code.index('011_indicator_registry_sources.sql'),
                            code.index('012_incremental_refresh.sql'))


@unittest.skipUnless(os.getenv('TALDAU_TEST_DB') == '1', 'Requires isolated test DB')
class IncrementalDatabaseTests(unittest.TestCase):
    setUp = framework.MultiIndicatorFrameworkTests.setUp
    tearDown = framework.MultiIndicatorFrameworkTests.tearDown
    make_ready_snapshot = framework.MultiIndicatorFrameworkTests.make_ready_snapshot

    def enable_only(self, key):
        with self.conn.cursor() as cur:
            cur.execute('UPDATE taldau.metadata_indicator_registry SET enabled=(indicator_key=%s)', (key,))

    def test_migration_replay_preserves_frozen_history(self):
        history = create_batch(self.conn, 'historical_fixture', year_start=2023, year_end=2026)
        before = [read_snapshot(self.conn, sid) for sid in history['snapshot_ids']]
        migration = (ROOT / 'dags/taldau_elt/sql/012_incremental_refresh.sql').read_text(encoding='utf-8')
        with self.conn.cursor() as cur:
            cur.execute('UPDATE taldau.metadata_indicator_registry SET year_end=2026 WHERE enabled')
            cur.execute(migration)
            cur.execute(migration)
            cur.execute('SELECT count(*) FROM taldau.metadata_indicator_registry WHERE enabled AND year_end=2100')
            self.assertEqual(cur.fetchone()[0], 8)
        self.assertEqual(before, [read_snapshot(self.conn, sid) for sid in history['snapshot_ids']])
        for year in (2027, 2028, 2035):
            result = create_batch(self.conn, 'future_' + str(year), year_start=year, year_end=year)
            self.assertEqual(result['indicator_count'], 8)

    def test_owner_survives_wave_and_failure_and_resume_is_idempotent(self):
        args = dict(year_start=2027, year_end=2027, source_as_of='2027-09-20',
                    orchestration_key=INCREMENTAL_OWNER)
        first = create_batch(self.conn, 'owner_first', **args)
        self.assertTrue(create_batch(self.conn, 'owner_first', **args)['already_exists'])
        with self.assertRaisesRegex(RuntimeError, 'must finish'):
            create_batch(self.conn, 'owner_second', **args)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_snapshots SET state='failed' WHERE batch_id='owner_first'")
            sid = first['snapshot_ids'][0]
            cur.execute("UPDATE taldau.bronze_snapshots SET discovery_complete=true WHERE snapshot_id=%s", (sid,))
            for state in ('complete', 'failed'):
                run = sid + ':' + state
                cur.execute('''INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                    SELECT %s,'statistics_'||indicator_key,source_config,'{}'
                    FROM taldau.bronze_snapshots WHERE snapshot_id=%s''', (run, sid))
                cur.execute('''INSERT INTO taldau.bronze_chunks(snapshot_id,chunk_key,run_id,state)
                    VALUES(%s,%s,%s,%s)''', (sid, state, run, state))
        with self.assertRaises(ValueError):
            finish_incremental_batch(self.conn, 'owner_first')
        create_batch(self.conn, 'owner_first', resume_failed=True, **args)
        self.assertEqual(read_snapshot(self.conn, sid)['state'], 'loading')
        with self.conn.cursor() as cur:
            cur.execute('SELECT chunk_key,state FROM taldau.bronze_chunks WHERE snapshot_id=%s ORDER BY chunk_key', (sid,))
            self.assertEqual(cur.fetchall(), [('complete', 'complete'), ('failed', 'queued')])
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_batches SET state='validated' WHERE batch_id='owner_first'")
        finish_incremental_batch(self.conn, 'owner_first')
        self.assertFalse(create_batch(self.conn, 'owner_second', **args).get('already_exists', False))

    def test_historical_batch_cannot_be_repurposed(self):
        create_batch(self.conn, 'history', year_start=2023, year_end=2026)
        with self.assertRaisesRegex(ValueError, 'Historical batches'):
            create_batch(self.conn, 'history', year_start=2023, year_end=2026,
                         source_as_of='2027-01-20', orchestration_key=INCREMENTAL_OWNER)

    def test_period_cutoff_uses_frequency_and_annual_semantics(self):
        cases = [('monthly', 'period', '082027', True), ('monthly', 'period', '092027', False),
                 ('quarterly', 'period', '062027', True), ('quarterly', 'period', '092027', False),
                 ('annual', 'point_in_time_start_period', '122027', True),
                 ('annual', 'point_in_time_start_period', '122028', False)]
        with self.conn.cursor() as cur:
            for frequency, semantics, period, expected in cases:
                config = {'frequency': frequency, 'period_semantics': semantics, 'incremental_as_of': '2027-09-20'}
                cur.execute('SELECT taldau.generic_period_available(%s,%s)', (period, Json(config)))
                self.assertEqual(cur.fetchone()[0], expected)

    def test_discovery_staging_retry_and_revisions_use_only_source_periods(self):
        self.enable_only('grp')  # Single dimension: discovery raw supplies all observation cells.
        args = dict(year_start=2027, year_end=2027, source_as_of='2027-09-20')
        nodes = [{'id': '741880', 'text': 'Fixture', 'leaf': True,
                  '122026': '202604', 'y122026': '99',
                  '032027': '202701', 'y032027': '10',
                  '092027': '202703', 'y092027': 'future-placeholder'}]
        from taldau_elt.loader import request_hash, tree_params
        def fixture(batch, value):
            sid = create_batch(self.conn, batch, **args)['snapshot_ids'][0]
            snapshot = read_snapshot(self.conn, sid)
            config = snapshot['source_config']
            params = tree_params(config, ['741880'], 'region')
            payload = [dict(nodes[0], y032027=value)]
            import json
            body = json.dumps(payload)
            with self.conn.cursor() as cur:
                cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                    (run_id,indicator_id,endpoint,period_id,request_params,request_hash,
                     response_data,response_text,response_hash,http_status,dimension,tree_depth)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,200,'region',0)''',
                    (snapshot['discovery_run_id'], config['indicator_id'], config['endpoint'],
                     config['period_id'], Json(params), request_hash(config['endpoint'], params),
                     Json(payload), body, uuid.uuid4().hex * 2))
            found = discover_snapshot(self.conn, sid, allow_http=False)
            self.assertEqual(found['available_periods'], ['032027'])
            with self.conn.cursor() as cur:
                cur.execute('SELECT chunk_id FROM taldau.bronze_chunks WHERE snapshot_id=%s', (sid,))
                chunk = cur.fetchone()[0]
            extract_chunk(self.conn, chunk, allow_http=False)
            self.assertTrue(extract_chunk(self.conn, chunk, allow_http=False)['already_complete'])
            self.assertEqual(discover_snapshot(self.conn, sid, allow_http=False)['http_requests'], 0)
            self.assertEqual(validate_batch(self.conn, batch)['state'], 'validated')
            publish_batch(self.conn, batch, auto_publish=True)
            with self.conn.cursor() as cur:
                cur.execute('SELECT period_code FROM taldau.staging_observation_cells WHERE snapshot_id=%s', (sid,))
                self.assertEqual(cur.fetchall(), [('032027',)])
            return sid
        old = fixture('refresh_one', '10')
        new = fixture('refresh_two', '11')
        self.assertNotEqual(old, new)
        with self.conn.cursor() as cur:
            cur.execute("SELECT value,source_snapshot_id FROM taldau.gold_fact_observations WHERE indicator_key='grp'")
            self.assertEqual(cur.fetchall(), [(11, new)])

    def test_batch_publication_is_atomic_and_repeated_validation_preserves_published(self):
        self.enable_only('grp')
        batch, first = self.make_ready_snapshot('grp', {'region': '741880'}, period='062027',
                                               year_start=2027, year_end=2027)
        self.enable_only('investments_fixed_assets')
        _, second = self.make_ready_snapshot('investments_fixed_assets',
            {'kato': '1', 'krp': '2', 'sif': '3', 'gsvziok': '4'}, period='082027', reporting=1070,
            year_start=2027, year_end=2027)
        with self.conn.cursor() as cur:
            cur.execute('UPDATE taldau.bronze_snapshots SET batch_id=%s WHERE snapshot_id=%s', (batch, second))
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'validated')
        with self.conn.cursor() as cur:
            cur.execute('''CREATE FUNCTION pg_temp.reject_second_indicator() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN IF NEW.indicator_key='investments_fixed_assets' THEN
                  RAISE EXCEPTION 'fixture second indicator failure'; END IF; RETURN NEW; END $$''')
            cur.execute('''CREATE TRIGGER reject_second BEFORE INSERT ON taldau.gold_fact_observations
                FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_second_indicator()''')
        with self.assertRaisesRegex(psycopg2.Error, 'second indicator'):
            publish_batch(self.conn, batch, auto_publish=True)
        with self.conn.cursor() as cur:
            cur.execute('SELECT state FROM taldau.bronze_batches WHERE batch_id=%s', (batch,))
            self.assertEqual(cur.fetchone()[0], 'validated')
            for table in ('silver_observations', 'gold_fact_observations'):
                cur.execute('SELECT count(*) FROM taldau.' + table + ' WHERE source_snapshot_id IN (%s,%s)', (first, second))
                self.assertEqual(cur.fetchone()[0], 0)
            cur.execute('DROP TRIGGER reject_second ON taldau.gold_fact_observations')
        self.assertTrue(publish_batch(self.conn, batch, auto_publish=True)['published'])
        self.assertTrue(publish_batch(self.conn, batch, auto_publish=True)['published'])
        self.assertEqual(validate_batch(self.conn, batch)['state'], 'published')

    def test_batch_rolls_back_on_gold_count_or_content_mismatch(self):
        self.enable_only('grp')
        batch, sid = self.make_ready_snapshot('grp', {'region': '741880'}, period='062027',
                                              year_start=2027, year_end=2027)
        validate_batch(self.conn, batch)
        for body, message in (("RETURN NULL;", 'Gold publication lost rows'),
                              ("NEW.value:=NEW.value+1; RETURN NEW;", 'Gold/Silver snapshot mismatch')):
            with self.subTest(message=message):
                with self.conn.cursor() as cur:
                    cur.execute('CREATE OR REPLACE FUNCTION pg_temp.corrupt_gold() RETURNS trigger '
                                'LANGUAGE plpgsql AS $$ BEGIN ' + body + ' END $$')
                    cur.execute('''CREATE TRIGGER corrupt_gold BEFORE INSERT ON taldau.gold_fact_observations
                        FOR EACH ROW EXECUTE FUNCTION pg_temp.corrupt_gold()''')
                with self.assertRaisesRegex(psycopg2.Error, message):
                    publish_batch(self.conn, batch, auto_publish=True)
                with self.conn.cursor() as cur:
                    cur.execute('SELECT state FROM taldau.bronze_batches WHERE batch_id=%s', (batch,))
                    self.assertEqual(cur.fetchone()[0], 'validated')
                    cur.execute('SELECT count(*) FROM taldau.silver_observations WHERE source_snapshot_id=%s', (sid,))
                    self.assertEqual(cur.fetchone()[0], 0)
                    cur.execute('DROP TRIGGER corrupt_gold ON taldau.gold_fact_observations')


if __name__ == '__main__':
    unittest.main()
