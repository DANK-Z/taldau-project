"""Offline registry, generic grain, isolation, validation and publication tests."""
from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
import sys
import unittest
import uuid
from unittest.mock import patch

import psycopg2
from psycopg2.extras import Json

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'dags'))
from taldau_elt.statistics import (WAVE_SIZE,batch_wave_outcome,create_batch,enabled_indicators,
    pending_batch_chunks,publish_snapshot,read_snapshot,validate_batch)
from test_investments_snapshots import SandboxConnection


def db_connection():
    return psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=os.getenv('PGPORT','5434'),
        dbname=os.getenv('PGDATABASE','taldau'),user=os.getenv('PGUSER','taldau'),password=os.getenv('PGPASSWORD'))


@unittest.skipUnless(os.getenv('TALDAU_TEST_DB')=='1','Requires isolated test DB')
class MultiIndicatorFrameworkTests(unittest.TestCase):
    def setUp(self):
        self.raw=db_connection()
        self.conn=SandboxConnection(self.raw)
        self.guard=patch('requests.sessions.Session.request',side_effect=AssertionError('HTTP prohibited'))
        self.http=self.guard.start()

    def tearDown(self):
        self.raw.rollback()
        self.raw.close()
        self.http.assert_not_called()
        self.guard.stop()

    def add_simple_indicator(self,key=None):
        key=key or 'fixture_'+uuid.uuid4().hex[:12]
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO taldau.metadata_indicator_registry
              (indicator_key,display_name,pipeline_type,indicator_id,period_id,endpoint,dimensions,roots,
               extraction_config,enabled,year_start,year_end)
              VALUES(%s,'Fixture','region_metric',900001,8,'https://invalid.test/offline',
               '[{"key":"kato","dic_id":68}]','{"kato":"741880"}',
               '{"strategy":"tree_cube","measure_id":1,"idx":3,"frequency":"monthly",
                 "period_code_regex":"^(0[1-9]|1[0-2])[0-9]{4}$","expected_periods_per_year":12}',true,2023,2026)""",(key,))
        return key

    def test_registry_has_all_verified_enabled_configurations(self):
        rows=enabled_indicators(self.conn)
        expected={
            'investments_fixed_assets':(701827,8,'cube','monthly','cumulative',3,12,
                [('kato',68),('krp',90),('sif',459),('gsvziok',4043)],
                {'kato':'741880','krp':'741927','sif':'807855','gsvziok':'19202525'}),
            'population':(703831,7,'region_metric','annual','point_in_time_start_period',3,1,
                [('region',67),('locality_type',749),('sex',576),('population_group',1433)],
                {'region':'741880','locality_type':'741917','sex':'741935','population_group':'3699122'}),
            'average_salary':(702972,5,'region_metric','quarterly','period',0,4,
                [('region',68),('economic_activity',859),('economy_sector',681)],
                {'region':'741880','economic_activity':'741885','economy_sector':'808076'}),
            'grp':(2709379,9,'region_metric','quarterly','cumulative',0,4,
                [('region',67)],{'region':'741880'}),
            'agriculture':(701189,8,'cube','monthly','cumulative',0,12,
                [('region',67),('producer_category',488),('price_type',773)],
                {'region':'741880','producer_category':'450122','price_type':'734928'}),
            'industry':(701592,8,'cube','monthly','cumulative',1,12,
                [('region',68),('enterprise_size',90),('economic_activity',4303)],
                {'region':'741880','enterprise_size':'741927','economic_activity':'3079117'}),
            'trade':(2709782,4,'cube','monthly','period',0,12,
                [('region',67),('period_relation',848)],
                {'region':'741880','period_relation':'2695732'}),
            'construction':(701885,8,'cube','monthly','cumulative',0,12,
                [('region',68),('enterprise_size',90),('construction_work_type',71)],
                {'region':'741880','enterprise_size':'741927','construction_work_type':'741919'}),
        }
        self.assertEqual({r['indicator_key'] for r in rows},set(expected))
        for row in rows:
            config=row['source_config']
            actual=(row['indicator_id'],row['period_id'],row['pipeline_type'],config['frequency'],
                    config['period_semantics'],config['idx'],config['expected_periods_per_year'],
                    [(d['key'],d['dic_id']) for d in row['dimensions']],row['roots'])
            self.assertEqual(actual,expected[row['indicator_key']],row['indicator_key'])
            self.assertEqual(row['endpoint'],'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData')
            self.assertEqual(config['measure_id'],1)
            self.assertTrue(row['dimensions'][0]['chunk'])
            self.assertEqual((row['year_start'],row['year_end']),(2023,2026))
        with self.conn.cursor() as cur:
            cur.execute("""SELECT pipeline_id,pipeline_type,config->>'period_semantics'
                FROM taldau.metadata_elt_pipelines WHERE pipeline_id LIKE 'statistics_%' ORDER BY pipeline_id""")
            pipelines=cur.fetchall()
            cur.execute("SELECT count(*) FROM taldau.metadata_elt_pipelines WHERE pipeline_id='inv_fixed_assets_monthly'")
            legacy_count=cur.fetchone()[0]
        self.assertEqual({row[0] for row in pipelines},{'statistics_'+key for key in expected})
        self.assertTrue(all(row[2] == expected[row[0].removeprefix('statistics_')][4] for row in pipelines))
        self.assertEqual(legacy_count,1)

    def test_independent_snapshots_failure_and_resume(self):
        other=self.add_simple_indicator()
        batch='batch_'+uuid.uuid4().hex
        result=create_batch(self.conn,batch)
        self.assertEqual(result['indicator_count'],9)
        with self.conn.cursor() as cur:
            cur.execute("SELECT snapshot_id,indicator_key FROM taldau.bronze_snapshots WHERE batch_id=%s ORDER BY indicator_key",(batch,))
            snapshots={key:sid for sid,key in cur.fetchall()}
            cur.execute("UPDATE taldau.bronze_snapshots SET state='failed',last_error='fixture' WHERE snapshot_id=%s",
                        (snapshots[other],))
        self.assertEqual(read_snapshot(self.conn,snapshots['investments_fixed_assets'])['state'],'prepared')
        create_batch(self.conn,batch,resume_failed=True)
        self.assertEqual(read_snapshot(self.conn,snapshots[other])['state'],'prepared')
        self.assertEqual(read_snapshot(self.conn,snapshots['investments_fixed_assets'])['state'],'prepared')

    def test_frozen_config_and_scope_are_immutable(self):
        batch='batch_'+uuid.uuid4().hex
        create_batch(self.conn,batch)
        with self.conn.cursor() as cur:
            cur.execute("""SELECT source_config FROM taldau.bronze_snapshots
                WHERE batch_id=%s AND indicator_key='investments_fixed_assets'""",(batch,))
            frozen=cur.fetchone()[0]
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.metadata_indicator_registry SET endpoint='https://changed.invalid' WHERE indicator_key='investments_fixed_assets'")
        create_batch(self.conn,batch)
        with self.conn.cursor() as cur:
            cur.execute("""SELECT source_config FROM taldau.bronze_snapshots
                WHERE batch_id=%s AND indicator_key='investments_fixed_assets'""",(batch,))
            self.assertEqual(cur.fetchone()[0],frozen)
        with self.assertRaisesRegex(ValueError,'scope is immutable'):
            create_batch(self.conn,batch,year_start=2024,year_end=2026)

    def make_ready_snapshot(self,key,coordinates,*,raw_value='10',status='numeric',period='122025',reporting=1069,
                            year_start=2023,year_end=2026):
        batch='batch_'+uuid.uuid4().hex
        create_batch(self.conn,batch,year_start=year_start,year_end=year_end)
        with self.conn.cursor() as cur:
            cur.execute("SELECT snapshot_id,discovery_run_id FROM taldau.bronze_snapshots WHERE batch_id=%s AND indicator_key=%s",(batch,key))
            sid,discovery=cur.fetchone()
            cur.execute("UPDATE taldau.bronze_snapshots SET discovery_complete=true,state='loading',expected_chunks=1 WHERE snapshot_id=%s",(sid,))
            run=sid+':fixture'
            cur.execute("""INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope,status)
              SELECT %s,'statistics_'||indicator_key,source_config,'{}','bronze_complete'
              FROM taldau.bronze_snapshots WHERE snapshot_id=%s""",(run,sid))
            cur.execute("""INSERT INTO taldau.bronze_chunks(snapshot_id,chunk_key,coordinates,run_id,state,completed_at,staged_at,raw_count)
              VALUES(%s,'fixture','{}',%s,'complete',now(),now(),0) RETURNING chunk_id""",(sid,run))
            chunk=cur.fetchone()[0]
            value=raw_value if status=='numeric' else None
            cur.execute("""INSERT INTO taldau.staging_observation_cells
              (snapshot_id,chunk_id,run_id,raw_id,node_ordinal,indicator_key,indicator_id,period_code,
               reporting_period,reporting_period_text,coordinates,coordinate_hash,has_value,has_period,
               raw_value,value,value_status,value_measure)
              SELECT %s,%s,%s,id,1,%s,indicator_id,%s,%s,%s,%s,md5(%s::jsonb::text),true,true,%s,%s,%s,'fixture'
              FROM taldau.bronze_taldau_api_raw LIMIT 1""",
              (sid,chunk,run,key,period,reporting,str(reporting),Json(coordinates),Json(coordinates),raw_value,value,status))
            for ordinal,(dimension,member_id) in enumerate(coordinates.items(),1):
                cur.execute("""INSERT INTO taldau.bronze_snapshot_members
                  (snapshot_id,run_id,raw_id,node_ordinal,dimension,member_id,member_name,parent_id,tree_depth,leaf,terms)
                  SELECT %s,%s,id,%s,%s,%s,%s,NULL,0,true,'' FROM taldau.bronze_taldau_api_raw LIMIT 1""",
                  (sid,run,ordinal,dimension,int(member_id),dimension+' '+str(member_id)))
        return batch,sid

    def test_generic_grain_preserves_simple_and_cube_dimensions(self):
        simple=self.add_simple_indicator()
        _,simple_sid=self.make_ready_snapshot(simple,{'kato':'268012'})
        _,cube_sid=self.make_ready_snapshot('investments_fixed_assets',
            {'kato':'268012','krp':'741927','sif':'807855','gsvziok':'19202525'})
        with self.conn.cursor() as cur:
            cur.execute("SELECT coordinates FROM taldau.staging_observation_cells WHERE snapshot_id=%s",(simple_sid,))
            self.assertEqual(cur.fetchone()[0],{'kato':'268012'})
            cur.execute("SELECT coordinates FROM taldau.staging_observation_cells WHERE snapshot_id=%s",(cube_sid,))
            self.assertEqual(set(cur.fetchone()[0]),{'kato','krp','sif','gsvziok'})

    def test_x_is_retained_but_unknown_and_duplicate_block(self):
        _,sid=self.make_ready_snapshot('investments_fixed_assets',
            {'kato':'268012','krp':'1','sif':'2','gsvziok':'3'})
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO taldau.staging_observation_cells
              SELECT snapshot_id,chunk_id,run_id,raw_id,node_ordinal+1,indicator_key,indicator_id,'112025',
               1070,'1070',coordinates,coordinate_hash,has_value,has_period,
               'x',NULL,'x',value_measure FROM taldau.staging_observation_cells WHERE snapshot_id=%s""",(sid,))
            cur.execute("SELECT taldau.validate_snapshot(%s)",(sid,))
            self.assertTrue(cur.fetchone()[0]['valid'])
            cur.execute("""UPDATE taldau.staging_observation_cells SET raw_value='?',value_status='invalid'
                WHERE snapshot_id=%s AND node_ordinal=2""",(sid,))
            cur.execute("SELECT taldau.validate_snapshot(%s)",(sid,))
            self.assertFalse(cur.fetchone()[0]['valid'])
            cur.execute("SELECT violations FROM taldau.quality_snapshot_checks WHERE snapshot_id=%s AND check_name='unknown_non_numeric'",(sid,))
            self.assertEqual(cur.fetchone()[0],1)

    def test_publish_is_idempotent_scoped_and_atomic(self):
        _,older=self.make_ready_snapshot('investments_fixed_assets',
            {'kato':'268012','krp':'1','sif':'2','gsvziok':'3'},period='122024',reporting=1032,
            year_start=2024,year_end=2024)
        self.assertEqual(publish_snapshot(self.conn,older),1)
        _,sid=self.make_ready_snapshot('investments_fixed_assets',
            {'kato':'268012','krp':'1','sif':'2','gsvziok':'3'},year_start=2025,year_end=2025)
        self.assertEqual(publish_snapshot(self.conn,sid),1)
        self.assertEqual(publish_snapshot(self.conn,sid),1)
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*),max(value) FROM taldau.gold_fact_observations WHERE source_snapshot_id=%s",(sid,))
            self.assertEqual(cur.fetchone(),(1,Decimal('10')))
            cur.execute("SELECT count(*) FROM taldau.gold_fact_observations WHERE source_snapshot_id=%s",(older,))
            self.assertEqual(cur.fetchone()[0],1)
            cur.execute("UPDATE taldau.bronze_snapshots SET state='validated' WHERE snapshot_id=%s",(sid,))
            cur.execute("""CREATE FUNCTION pg_temp.reject_generic_gold() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'fixture gold failure'; END $$""")
            cur.execute("""CREATE TRIGGER reject_generic_gold BEFORE INSERT ON taldau.gold_fact_observations
                FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_generic_gold()""")
        with self.assertRaises(psycopg2.Error):
            publish_snapshot(self.conn,sid)
        with self.conn.cursor() as cur:
            cur.execute("SELECT state FROM taldau.bronze_snapshots WHERE snapshot_id=%s",(sid,))
            self.assertEqual(cur.fetchone()[0],'validated')
            cur.execute("SELECT count(*) FROM taldau.silver_observations WHERE source_snapshot_id=%s",(sid,))
            self.assertEqual(cur.fetchone()[0],1)

    def test_older_overlapping_snapshot_cannot_replace_newer_publication(self):
        coordinates={'kato':'268012','krp':'1','sif':'2','gsvziok':'3'}
        _,old=self.make_ready_snapshot('investments_fixed_assets',coordinates,year_start=2025,year_end=2025)
        self.assertEqual(publish_snapshot(self.conn,old),1)
        _,new=self.make_ready_snapshot('investments_fixed_assets',coordinates,raw_value='11',
                                       year_start=2025,year_end=2025)
        self.assertEqual(publish_snapshot(self.conn,new),1)
        with self.assertRaisesRegex(psycopg2.Error,'Newer data'):
            publish_snapshot(self.conn,old)

    def test_wave_bound_and_failed_indicator_does_not_block_other(self):
        other=self.add_simple_indicator()
        batch='batch_'+uuid.uuid4().hex
        create_batch(self.conn,batch)
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE taldau.bronze_snapshots SET discovery_complete=true,state='loading',expected_chunks=0
                WHERE batch_id=%s""",(batch,))
            cur.execute("SELECT snapshot_id,indicator_key,source_config FROM taldau.bronze_snapshots WHERE batch_id=%s",(batch,))
            rows=cur.fetchall()
            for sid,key,config in rows:
                for n in range(130 if key=='investments_fixed_assets' else 1):
                    run=f'{sid}:fixture:{n}'
                    cur.execute("""INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                      VALUES(%s,%s,%s,'{}')""",(run,'statistics_'+key,Json(config)))
                    cur.execute("INSERT INTO taldau.bronze_chunks(snapshot_id,chunk_key,run_id) VALUES(%s,%s,%s)",
                                (sid,str(n),run))
            cur.execute("UPDATE taldau.bronze_snapshots SET expected_chunks=(SELECT count(*) FROM taldau.bronze_chunks c WHERE c.snapshot_id=bronze_snapshots.snapshot_id) WHERE batch_id=%s",(batch,))
        self.assertEqual(len(pending_batch_chunks(self.conn,batch)),WAVE_SIZE)
        with self.assertRaisesRegex(ValueError,'Maximum wave size'):
            pending_batch_chunks(self.conn,batch,WAVE_SIZE+1)
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE taldau.bronze_chunks SET state='failed' WHERE chunk_id=(
              SELECT min(c.chunk_id) FROM taldau.bronze_chunks c JOIN taldau.bronze_snapshots s USING(snapshot_id)
              WHERE s.batch_id=%s AND s.indicator_key=%s)""",(batch,other))
        self.assertEqual(batch_wave_outcome(self.conn,batch,[]),'continue')
        with self.conn.cursor() as cur:
            cur.execute("SELECT state FROM taldau.bronze_snapshots WHERE batch_id=%s AND indicator_key=%s",(batch,other))
            self.assertEqual(cur.fetchone()[0],'failed')

    def test_malformed_raw_and_partial_current_year_are_reported_offline(self):
        _,sid=self.make_ready_snapshot('investments_fixed_assets',
            {'kato':'268012','krp':'1','sif':'2','gsvziok':'3'},period='082026',reporting=2008)
        snapshot=read_snapshot(self.conn,sid)
        params={'p_measure_id':'1','p_index_id':'701827','p_period_id':'8','p_terms':'741880,741927,807855,19202525',
                'p_term_id':'741880','p_dicIds':'68,90,459,4043','idx':'3','p_parent_id':''}
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO taldau.bronze_taldau_api_raw
              (run_id,indicator_id,endpoint,period_id,request_params,request_hash,response_data,response_text,
               response_hash,http_status,dimension,tree_depth)
              VALUES(%s,701827,%s,8,%s,%s,'{}','{}',%s,200,'kato',0) RETURNING id""",
              (snapshot['discovery_run_id'],snapshot['source_config']['endpoint'],Json(params),'a'*64,'b'*64))
            raw_id=cur.fetchone()[0]
            cur.execute("""INSERT INTO taldau.bronze_request_tasks(run_id,request_hash,state,dimension,request_params,raw_id,completed_at)
              VALUES(%s,%s,'complete','kato',%s,%s,now())""",(snapshot['discovery_run_id'],'a'*64,Json(params),raw_id))
            cur.execute("SELECT taldau.validate_snapshot(%s)",(sid,))
            self.assertFalse(cur.fetchone()[0]['valid'])
            cur.execute("SELECT violations FROM taldau.quality_snapshot_checks WHERE snapshot_id=%s AND check_name='malformed_responses'",(sid,))
            self.assertEqual(cur.fetchone()[0],1)
            cur.execute("SELECT severity,details FROM taldau.quality_snapshot_checks WHERE snapshot_id=%s AND check_name='missing_periods'",(sid,))
            severity,details=cur.fetchone()
            self.assertEqual(severity,'warning')
            self.assertTrue(details['current_year_partial_allowed'])

    def test_real_taldau_period_codes(self):
        cases=[
            ('012026','monthly','2026-01-01','2026-01-31'),
            ('032026','quarterly','2026-01-01','2026-03-31'),
            ('062026','quarterly','2026-04-01','2026-06-30'),
            ('092026','quarterly','2026-07-01','2026-09-30'),
            ('122026','quarterly','2026-10-01','2026-12-31'),
            ('122025','annual','2025-01-01','2025-12-31'),
            ('Q12026','quarterly','2026-01-01','2026-03-31'),
            ('2025','annual','2025-01-01','2025-12-31'),
        ]
        with self.conn.cursor() as cur:
            for code,frequency,start,end in cases:
                cur.execute("SELECT taldau.generic_period_start(%s,%s),taldau.generic_period_end(%s,%s)",
                            (code,frequency,code,frequency))
                actual=cur.fetchone()
                self.assertEqual(tuple(map(str,actual)),(start,end),(code,frequency))

    def test_current_partial_year_is_not_blocking_and_historical_gap_is_warning(self):
        coordinates={'kato':'268012','krp':'1','sif':'2','gsvziok':'3'}
        _,current=self.make_ready_snapshot('investments_fixed_assets',coordinates,
            period='082026',reporting=2008,year_start=2026,year_end=2026)
        _,historical=self.make_ready_snapshot('investments_fixed_assets',coordinates,
            period='012025',reporting=2001,year_start=2025,year_end=2025)
        with self.conn.cursor() as cur:
            cur.execute("SELECT taldau.validate_snapshot(%s)",(current,))
            self.assertTrue(cur.fetchone()[0]['valid'])
            cur.execute("""SELECT severity,violations,details->>'current_year_partial_allowed'
                FROM taldau.quality_snapshot_checks
                WHERE snapshot_id=%s AND check_name='missing_periods'""",(current,))
            self.assertEqual(cur.fetchone(),('warning',0,'true'))
            cur.execute("SELECT taldau.validate_snapshot(%s)",(historical,))
            self.assertTrue(cur.fetchone()[0]['valid'])
            cur.execute("""SELECT severity,violations,details->>'current_year_partial_allowed'
                FROM taldau.quality_snapshot_checks
                WHERE snapshot_id=%s AND check_name='missing_periods'""",(historical,))
            self.assertEqual(cur.fetchone(),('warning',11,'false'))

    def test_indicator_source_migration_is_idempotent(self):
        root=Path(__file__).resolve().parents[1]
        migration=(root/'dags/taldau_elt/sql/011_indicator_registry_sources.sql').read_text(encoding='utf-8')
        with self.conn.cursor() as cur:
            cur.execute(migration)
            cur.execute(migration)
            cur.execute("SELECT count(*),count(DISTINCT indicator_key) FROM taldau.metadata_indicator_registry WHERE enabled")
            self.assertEqual(cur.fetchone(),(8,8))
            cur.execute("SELECT count(*) FROM taldau.metadata_elt_pipelines WHERE pipeline_id LIKE 'statistics_%'")
            self.assertEqual(cur.fetchone()[0],8)


class StaticGenericFrameworkTests(unittest.TestCase):
    def test_dag_is_registry_driven_and_catalog_has_no_fake_ids(self):
        root=Path(__file__).resolve().parents[1]
        dag=(root/'dags/taldau_statistics_2023_2026.py').read_text(encoding='utf-8')
        self.assertNotIn('investments_fixed_assets',dag)
        sql=(root/'dags/taldau_elt/sql/010_multi_indicator_framework.sql').read_text(encoding='utf-8')
        for key in ('population','average_salary','grp','agriculture','industry','trade','construction'):
            self.assertRegex(sql,rf"\('{key}'.*NULL,NULL,NULL,NULL,NULL,NULL,false")

    def test_migration_loaders_apply_011_after_010(self):
        root=Path(__file__).resolve().parents[1]
        for relative in ('tools/manage_statistics_batch.py','tools/manage_investments_snapshot.py'):
            text=(root/relative).read_text(encoding='utf-8')
            self.assertIn('010_multi_indicator_framework.sql',text)
            self.assertIn('011_indicator_registry_sources.sql',text)
            self.assertLess(text.index('010_multi_indicator_framework.sql'),
                            text.index('011_indicator_registry_sources.sql'))


if __name__=='__main__': unittest.main()
