"""Multi-year tests use cached Bronze in the disposable PostgreSQL runner only."""
import os
import unittest
import uuid

import psycopg2
import test_investments_snapshots as fixtures
from taldau_elt.snapshots import create_snapshot,validate_snapshot,publish_snapshot,read_snapshot,discover_territories,MissingRawResponse
from taldau_elt.coverage import audit_bronze,coverage_plan,reuse_offline
from taldau_elt.reports import snapshot_summary


@unittest.skipUnless(os.getenv('TALDAU_TEST_DB')=='1','Requires isolated test DB')
class MultiYearTests(unittest.TestCase):
    setUp=fixtures.SnapshotTests.setUp
    tearDown=fixtures.SnapshotTests.tearDown
    insert_raw=fixtures.SnapshotTests.insert_raw
    complete=fixtures.SnapshotTests.complete

    def prepare_range(self,start=2023,end=2026):
        self.complete()
        validate_snapshot(self.conn,self.sid)
        target='range_'+uuid.uuid4().hex
        create_snapshot(self.conn,target,year_start=start,year_end=end,reuse_snapshot_id=self.sid)
        return target

    def test_multi_year_reuse_preserves_raw_and_source_and_partial_year(self):
        target=self.prepare_range()
        with self.conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM taldau.bronze_taldau_api_raw')
            raw_before=cur.fetchone()[0]
        plan=coverage_plan(self.conn,target)
        self.assertEqual(plan['missing_requests'],0)
        self.assertEqual(plan['expected_http_requests'],0)
        self.assertEqual(plan['chunks_fully_reusable'],2)
        self.assertEqual(plan['http_requests'],0)
        result=reuse_offline(self.conn,target)
        self.assertEqual(result['http_requests'],0)
        validation=validate_snapshot(self.conn,target)
        self.assertTrue(validation['valid'])
        report=snapshot_summary(self.conn,target)
        self.assertEqual((report['year_start'],report['year_end']),(2023,2026))
        self.assertEqual([y['year'] for y in report['years']],[2023,2024,2025,2026])
        self.assertTrue(0<report['years'][-1]['months_present']<12)
        self.assertEqual(report['years'][-1]['warning'],'partial_year')
        self.assertTrue(all(m['expected_ets_rows'] is None and m['delta'] is None
                            for m in report['months'] if m['year']!=2025))
        with self.conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM taldau.bronze_taldau_api_raw')
            self.assertEqual(cur.fetchone()[0],raw_before)
            cur.execute('''SELECT count(DISTINCT right(period_code,4)) FROM taldau.staging_inv_year_cells
                WHERE snapshot_id=%s GROUP BY raw_id,node_ordinal ORDER BY 1 DESC LIMIT 1''',(target,))
            self.assertEqual(cur.fetchone()[0],4)
            cur.execute('''SELECT count(*) FROM taldau.bronze_inv_request_tasks q JOIN taldau.bronze_taldau_api_raw r ON r.id=q.raw_id
                WHERE q.run_id LIKE %s AND q.run_id<>r.run_id''',(target+'%',))
            self.assertEqual(cur.fetchone()[0],plan['reusable_requests'])
        self.assertEqual(read_snapshot(self.conn,self.sid)['state'],'validated')
        self.http.assert_not_called()

    def test_missing_raw_only_reports_frontier_and_cannot_call_http(self):
        target=self.prepare_range()
        with self.conn.cursor() as cur:
            cur.execute('''DELETE FROM taldau.bronze_inv_reuse_raw x USING taldau.bronze_taldau_api_raw r
                WHERE x.snapshot_id=%s AND r.id=x.raw_id AND r.dimension='kato'
                AND r.request_params->>'p_parent_id'='' ''',(target,))
        plan=coverage_plan(self.conn,target)
        self.assertEqual(plan['missing_requests'],1)
        self.assertEqual(plan['expected_http_requests_lower_bound'],1)
        self.assertIsNone(plan['expected_http_requests'])
        self.assertFalse(plan['estimate_is_exact'])
        self.assertEqual(reuse_offline(self.conn,target)['http_requests'],0)
        self.assertEqual(read_snapshot(self.conn,target)['state'],'prepared')
        with self.assertRaises(MissingRawResponse):
            discover_territories(self.conn,target,allow_http=False)
        self.http.assert_not_called()

    def test_offline_audit_uses_observed_mapping_and_does_not_write(self):
        target=self.prepare_range()
        before=read_snapshot(self.conn,target)
        report=audit_bronze(self.conn,target,2023,2026,prepared=True)
        self.assertEqual(report['http_requests'],0)
        self.assertEqual([y['months_present'] for y in report['years'][:3]],[12,12,12])
        self.assertLess(report['years'][-1]['months_present'],12)
        self.assertTrue(any(p['period_code']=='122023' and p['reporting_period']=='995' for p in report['periods']))
        self.assertEqual(read_snapshot(self.conn,target),before)
        self.http.assert_not_called()

    def test_multi_year_publish_matches_gold_idempotently_and_preserves_outside_scope(self):
        target=self.prepare_range(start=2023,end=2024)
        reuse_offline(self.conn,target)
        expected=validate_snapshot(self.conn,target)['numeric_rows']
        with self.conn.cursor() as cur:
            cur.execute('SELECT row_to_json(v)::text FROM taldau.silver_inv_fixed_assets v ORDER BY 1')
            old_silver=cur.fetchall()
            cur.execute('SELECT row_to_json(v)::text FROM taldau.gold_fact_inv_fixed_assets v ORDER BY 1')
            old_gold=cur.fetchall()
        for _ in range(2):
            self.assertEqual(publish_snapshot(self.conn,target),expected)
            with self.conn.cursor() as cur:
                cur.execute('SELECT row_to_json(v)::text FROM taldau.silver_inv_fixed_assets v WHERE extract(year FROM period_date)=2025 ORDER BY 1')
                self.assertEqual(cur.fetchall(),old_silver)
                cur.execute('SELECT row_to_json(v)::text FROM taldau.gold_fact_inv_fixed_assets v WHERE reporting_period=1069 ORDER BY 1')
                self.assertEqual(cur.fetchall(),old_gold)
                cur.execute('''SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
                    FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=%s ORDER BY 1,2,3,4,5,6''',(target,))
                silver=cur.fetchall()
                cur.execute('''SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
                    FROM taldau.gold_v_inv_fixed_assets WHERE extract(year FROM start_date) BETWEEN 2023 AND 2024 ORDER BY 1,2,3,4,5,6''')
                self.assertEqual(cur.fetchall(),silver)
                self.assertEqual(len(silver),expected)

    def test_duplicates_and_period_id_conflicts_across_years_block_publish(self):
        target=self.prepare_range()
        reuse_offline(self.conn,target)
        with self.conn.cursor() as cur:
            cur.execute('''UPDATE taldau.staging_inv_year_cells a SET reporting_period=b.reporting_period
                FROM taldau.staging_inv_year_cells b WHERE a.snapshot_id=%s AND b.snapshot_id=a.snapshot_id
                  AND a.period_code='122024' AND b.period_code='122025'
                  AND a.raw_id=b.raw_id AND a.node_ordinal=b.node_ordinal''',(target,))
        with self.assertRaisesRegex(ValueError,'validation failed'):
            validate_snapshot(self.conn,target)
        with self.conn.cursor() as cur:
            cur.execute("SELECT check_name FROM taldau.quality_inv_snapshot_checks WHERE snapshot_id=%s AND violations>0",(target,))
            failed={r[0] for r in cur.fetchall()}
        self.assertIn('duplicate_natural_keys',failed)
        self.assertIn('conflicting_period_mapping',failed)

    def test_old_snapshot_cannot_overwrite_newer_data_in_overlapping_year(self):
        target=self.prepare_range()
        reuse_offline(self.conn,target)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_inv_snapshots SET created_at=now()-interval '10 years' WHERE snapshot_id=%s",(target,))
        with self.assertRaisesRegex(psycopg2.Error,'Newer data is already published'):
            publish_snapshot(self.conn,target)
        with self.conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=%s',(target,))
            self.assertEqual(cur.fetchone()[0],0)

    def test_same_source_reuse_can_replace_overlapping_year_without_being_stale(self):
        target=self.prepare_range()
        publish_snapshot(self.conn,self.sid)
        reuse_offline(self.conn,target)
        expected=validate_snapshot(self.conn,target)['numeric_rows']
        self.assertEqual(publish_snapshot(self.conn,target),expected)

    def test_scope_and_reuse_source_cannot_change_on_resume(self):
        target=self.prepare_range()
        with self.assertRaisesRegex(ValueError,'immutable'):
            create_snapshot(self.conn,target,year_start=2024,year_end=2026,reuse_snapshot_id=self.sid)
        with self.assertRaisesRegex(ValueError,'immutable'):
            create_snapshot(self.conn,target,year_start=2023,year_end=2026)

    def test_legacy_null_range_columns_still_publish_2025(self):
        self.complete()
        with self.conn.cursor() as cur:
            cur.execute('UPDATE taldau.bronze_inv_snapshots SET year_start=NULL,year_end=NULL WHERE snapshot_id=%s',(self.sid,))
        expected=validate_snapshot(self.conn,self.sid)['numeric_rows']
        report=snapshot_summary(self.conn,self.sid)
        self.assertEqual((report['year_start'],report['year_end']),(2025,2025))
        self.assertEqual(publish_snapshot(self.conn,self.sid),expected)

    def test_new_snapshot_name_cannot_make_stale_reused_raw_fresh(self):
        target=self.prepare_range()
        publish_snapshot(self.conn,self.sid)
        reuse_offline(self.conn,target)
        fresh='fresh_'+uuid.uuid4().hex
        with self.conn.cursor() as cur:
            cur.execute('''INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope,status,started_at)
                SELECT %s,pipeline_id,config,scope,'bronze_complete',now()+interval '1 hour'
                FROM taldau.bronze_extraction_runs WHERE run_id=%s''',(fresh,self.chunks[268012][1]))
            cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                (run_id,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                 response_text,response_hash,http_status,dimension,tree_depth,loaded_at)
                SELECT %s,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                    response_text,response_hash,http_status,dimension,tree_depth,now()+interval '1 hour'
                FROM taldau.bronze_taldau_api_raw WHERE run_id=%s''',(fresh,self.chunks[268012][1]))
            cur.execute('''UPDATE taldau.silver_inv_fixed_assets v SET source_run_id=%s,source_raw_id=new.id
                FROM taldau.bronze_taldau_api_raw old,taldau.bronze_taldau_api_raw new
                WHERE old.id=v.source_raw_id AND new.run_id=%s AND new.request_hash=old.request_hash
                  AND v.source_snapshot_id=%s''',(fresh,fresh,self.sid))
            # The new snapshot was created AFTER fresh publication, but its raw is older.
            cur.execute("UPDATE taldau.bronze_inv_snapshots SET created_at=now()+interval '2 hours' WHERE snapshot_id=%s",(target,))
        with self.assertRaisesRegex(psycopg2.Error,'Newer data is already published'):
            publish_snapshot(self.conn,target)

    def test_period_id_cannot_move_existing_gold_to_another_year(self):
        target=self.prepare_range(start=2023,end=2024)
        reuse_offline(self.conn,target)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.staging_inv_year_cells SET reporting_period=1069 WHERE snapshot_id=%s AND period_code='122023'",(target,))
            cur.execute('SELECT row_to_json(v)::text FROM taldau.gold_fact_inv_fixed_assets v ORDER BY 1')
            before=cur.fetchall()
        # Give the pre-existing Silver facts different keys so the PK does not hide the
        # separate Gold period-boundary guard. The incoming inventory remains unchanged.
        with self.conn.cursor() as cur:
            cur.execute('''UPDATE taldau.silver_inv_fixed_assets SET kato_id=741880
                WHERE indicator_id=701827 AND reporting_period=1069''')
        with self.assertRaisesRegex(psycopg2.Error,'Reporting period conflicts'):
            publish_snapshot(self.conn,target)
        with self.conn.cursor() as cur:
            cur.execute('SELECT row_to_json(v)::text FROM taldau.gold_fact_inv_fixed_assets v ORDER BY 1')
            self.assertEqual(cur.fetchall(),before)
            cur.execute('SELECT count(*) FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=%s',(target,))
            self.assertEqual(cur.fetchone()[0],0)


if __name__=='__main__': unittest.main()
