"""Offline-only snapshot integration tests. Synthetic inventory + cached pilot; full rollback."""
import hashlib
import json
import os
from pathlib import Path
import sys
import unittest
import uuid
from unittest.mock import patch

import psycopg2
from psycopg2.extras import Json

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'dags'))
from taldau_elt.loader import tree_params,request_hash
from taldau_elt.snapshots import (create_snapshot,discover_territories,extract_chunk,
    pending_chunk_ids,validate_snapshot,publish_snapshot,wave_outcome,read_snapshot)
from taldau_elt.reports import snapshot_summary,format_summary,write_summary


class SandboxConnection:
    """Contain application commits inside one outer test transaction using savepoints."""
    def __init__(self,conn):
        self.conn=conn
        self.savepoints=[]
    def cursor(self,*args,**kwargs): return self.conn.cursor(*args,**kwargs)
    def commit(self): pass
    def rollback(self): pass
    def __enter__(self):
        name='sp_'+uuid.uuid4().hex
        with self.conn.cursor() as cur: cur.execute('SAVEPOINT '+name)
        self.savepoints.append(name)
        return self
    def __exit__(self,typ,value,tb):
        name=self.savepoints.pop()
        with self.conn.cursor() as cur:
            if typ:
                cur.execute('ROLLBACK TO SAVEPOINT '+name)
            cur.execute('RELEASE SAVEPOINT '+name)
        return False


@unittest.skipUnless(os.getenv('TALDAU_TEST_DB')=='1','Requires local test opt-in')
class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.raw_conn=psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=os.getenv('PGPORT','5434'),
            dbname=os.getenv('PGDATABASE','taldau'),user=os.getenv('PGUSER','taldau'),password=os.getenv('PGPASSWORD'))
        self.conn=SandboxConnection(self.raw_conn)
        self.network=patch('requests.sessions.Session.request',side_effect=AssertionError('Network prohibited in snapshot tests'))
        self.http=self.network.start()
        self.sid='test_snapshot_'+uuid.uuid4().hex
        create_snapshot(self.conn,self.sid)
        self.config=read_snapshot(self.conn,self.sid)['config']
        terms=[self.config['roots'][d] for d in ('kato','krp','sif','gsvziok')]
        # This is explicitly a two-territory synthetic inventory, NOT evidence of country completeness.
        self.insert_raw(self.sid+':discovery','kato',terms,'',
            [{'id':'741880','text':'Test country','leaf':'false'}],0)
        self.insert_raw(self.sid+':discovery','kato',terms,'741880',
            [{'id':'268012','text':'Г.АСТАНА','leaf':'true'}],1)
        result=discover_territories(self.conn,self.sid)
        self.assertEqual(result['chunks'],2)
        with self.conn.cursor() as cur:
            cur.execute('SELECT territory_id,chunk_id,run_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=%s',(self.sid,))
            self.chunks={row[0]:(row[1],row[2]) for row in cur.fetchall()}
            cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                (run_id,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                 response_text,response_hash,http_status,dimension,tree_depth)
                SELECT %s,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                    response_text,response_hash,http_status,dimension,tree_depth
                FROM taldau.bronze_taldau_api_raw WHERE run_id='astana-2025-12-pilot-v1' AND dimension<>'kato' ''',
                (self.chunks[268012][1],))
        self.insert_raw(self.chunks[741880][1],'krp',terms,'',[],0)

    def tearDown(self):
        self.raw_conn.rollback()
        self.raw_conn.close()  # Releases any advisory locks left by an intentionally failed test.
        self.http.assert_not_called()
        self.network.stop()

    def insert_raw(self,run,dimension,terms,parent,nodes,depth):
        params=tree_params(self.config,terms,dimension,parent)
        body=json.dumps(nodes,ensure_ascii=False)
        with self.conn.cursor() as cur:
            cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                (run_id,indicator_id,endpoint,period_id,request_params,request_hash,response_data,response_text,
                 response_hash,http_status,dimension,tree_depth)
                VALUES(%s,701827,%s,8,%s,%s,%s::jsonb,%s,%s,200,%s,%s)''',
                (run,self.config['endpoint'],Json(params),request_hash(self.config['endpoint'],params),
                 body,body,hashlib.sha256(body.encode()).hexdigest(),dimension,depth))

    def complete(self):
        for chunk_id,_ in self.chunks.values():
            extract_chunk(self.conn,chunk_id)

    def checks(self):
        with self.conn.cursor() as cur:
            cur.execute('SELECT taldau.quality_validate_inv_snapshot(%s)',(self.sid,))
            result=cur.fetchone()[0]
            cur.execute('SELECT check_name,violations FROM taldau.quality_inv_snapshot_checks WHERE snapshot_id=%s AND violations>0',(self.sid,))
            return result,dict(cur.fetchall())

    def test_full_year_sql_and_atomic_publication_ignore_diagnostic_count(self):
        self.complete()
        result=validate_snapshot(self.conn,self.sid)
        self.assertTrue(result['valid'])
        self.assertNotEqual(result['numeric_rows'],651365)
        published=publish_snapshot(self.conn,self.sid)
        self.assertEqual(published,result['numeric_rows'])
        with self.conn.cursor() as cur:
            cur.execute('''SELECT count(*) FROM taldau.silver_inv_fixed_assets
                WHERE source_snapshot_id=%s AND kato_id=268012 AND reporting_period=1069''',(self.sid,))
            self.assertEqual(cur.fetchone()[0],504)
            cur.execute('''SELECT count(DISTINCT period_code),count(*) FILTER(WHERE raw_value='x' AND period_code='122025')
                FROM taldau.staging_inv_year_cells WHERE snapshot_id=%s''',(self.sid,))
            self.assertEqual(cur.fetchone(),(12,7))
            cur.execute('SELECT count(*) FROM taldau.quality_inv_month_diagnostics WHERE snapshot_id=%s',(self.sid,))
            self.assertEqual(cur.fetchone()[0],12)

    def test_validation_and_summary_stop_before_silver_publication(self):
        import tempfile
        self.complete()
        validation=validate_snapshot(self.conn,self.sid)
        report=snapshot_summary(self.conn,self.sid)
        self.assertEqual(report['snapshot_state'],'validated')
        self.assertTrue(report['ready_to_publish'])
        self.assertEqual(report['numeric_rows'],validation['numeric_rows'])
        self.assertEqual((report['territories_total'],report['chunks_total'],report['chunks_complete']),(2,2,2))
        self.assertEqual((report['chunks_failed'],report['invalid_rows'],report['duplicate_keys']),(0,0,0))
        self.assertEqual(report['blocking_checks_failed'],0)
        self.assertGreater(report['blocking_checks_run'],0)
        self.assertEqual(report['expected_ets_rows'],651365)
        december=next(m for m in report['months'] if m['period_code']=='122025')
        self.assertEqual((december['reporting_period'],december['numeric_rows'],december['x_rows']),(1069,504,7))
        self.assertEqual(december['delta'],504-71155)
        self.assertEqual([c['territory_id'] for c in report['chunks_without_facts']],[741880])
        self.assertIn('snapshot_state: validated',format_summary(report))
        with tempfile.TemporaryDirectory() as folder:
            output=Path(folder)/'summary.json'
            write_summary(report,output)
            self.assertEqual(json.loads(output.read_text(encoding='utf-8')),report)
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM taldau.silver_inv_fixed_assets WHERE source_run_id='astana-2025-12-pilot-v1'")
            self.assertEqual(cur.fetchone()[0],504)
            cur.execute('SELECT count(*) FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=%s',(self.sid,))
            self.assertEqual(cur.fetchone()[0],0)
        # Only this explicit call publishes; summaries themselves are read-only.
        publish_snapshot(self.conn,self.sid)
        self.assertEqual(snapshot_summary(self.conn,self.sid)['snapshot_state'],'published')

    def publication_image(self):
        """Include values, provenance and timestamps to detect partial writes on failure."""
        with self.conn.cursor() as cur:
            result=[]
            for table in ('taldau.silver_inv_fixed_assets','taldau.gold_fact_inv_fixed_assets',
                          'taldau.gold_dim_inv_member','taldau.gold_dim_inv_period'):
                cur.execute('SELECT row_to_json(t)::text FROM '+table+' t ORDER BY 1')
                result.append(cur.fetchall())
            cur.execute('SELECT state,published_at FROM taldau.bronze_inv_snapshots WHERE snapshot_id=%s',(self.sid,))
            result.append(cur.fetchone())
            return result

    def test_gold_matches_silver_and_republication_is_idempotent(self):
        self.complete()
        expected=validate_snapshot(self.conn,self.sid)['numeric_rows']
        with self.conn.cursor() as cur:
            # Existing facts outside the target year must survive publication.
            cur.execute('''INSERT INTO taldau.gold_dim_inv_period
                SELECT indicator_id,-2024,'122024','2024-01-01','2024-12-31',period_type
                FROM taldau.gold_dim_inv_period LIMIT 1''')
            cur.execute('''INSERT INTO taldau.gold_fact_inv_fixed_assets
                (indicator_id,reporting_period,kato_key,krp_key,sif_key,gsvziok_key,value,value_measure,source_run_id,source_raw_id)
                SELECT indicator_id,-2024,kato_key,krp_key,sif_key,gsvziok_key,value,value_measure,source_run_id,source_raw_id
                FROM taldau.gold_fact_inv_fixed_assets LIMIT 1''')
        first=None
        for _ in range(2):
            self.assertEqual(publish_snapshot(self.conn,self.sid),expected)
            with self.conn.cursor() as cur:
                cur.execute('''SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
                    FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=%s ORDER BY 1,2,3,4,5,6''',(self.sid,))
                silver=cur.fetchall()
                cur.execute('''SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
                    FROM taldau.gold_v_inv_fixed_assets WHERE indicator_id=701827
                    AND extract(year FROM start_date)=2025 ORDER BY 1,2,3,4,5,6''')
                gold=cur.fetchall()
                self.assertEqual(len(gold),expected)
                self.assertEqual(silver,gold)
                self.assertEqual(len({row[:-1] for row in gold}),expected)
                if first is not None: self.assertEqual(gold,first)
                first=gold
                cur.execute('SELECT count(*) FROM taldau.gold_fact_inv_fixed_assets WHERE reporting_period=-2024')
                self.assertEqual(cur.fetchone()[0],1)

    def test_gold_failure_rolls_back_silver_gold_and_snapshot_state(self):
        self.complete()
        validate_snapshot(self.conn,self.sid)
        before=self.publication_image()
        # Fault injection after Silver publication: keep the count but corrupt Gold values.
        with self.conn.cursor() as cur:
            cur.execute('''CREATE FUNCTION pg_temp.corrupt_gold() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN NEW.value:=NEW.value+1; RETURN NEW; END $$''')
            cur.execute('''CREATE TRIGGER test_corrupt_gold BEFORE INSERT ON taldau.gold_fact_inv_fixed_assets
                FOR EACH ROW EXECUTE FUNCTION pg_temp.corrupt_gold()''')
        with self.assertRaisesRegex(psycopg2.Error,'Gold/Silver snapshot mismatch'):
            publish_snapshot(self.conn,self.sid)
        self.assertEqual(self.publication_image(),before)

    def test_gold_row_loss_raises_and_rolls_back(self):
        self.complete()
        validate_snapshot(self.conn,self.sid)
        before=self.publication_image()
        with self.conn.cursor() as cur:
            cur.execute('''CREATE FUNCTION pg_temp.skip_gold() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RETURN NULL; END $$''')
            cur.execute('''CREATE TRIGGER test_skip_gold BEFORE INSERT ON taldau.gold_fact_inv_fixed_assets
                FOR EACH ROW EXECUTE FUNCTION pg_temp.skip_gold()''')
        with self.assertRaisesRegex(psycopg2.Error,'Gold row loss'):
            publish_snapshot(self.conn,self.sid)
        self.assertEqual(self.publication_image(),before)

    def test_missing_member_cannot_silently_reuse_existing_gold_dimension(self):
        self.complete()
        publish_snapshot(self.conn,self.sid)
        with self.conn.cursor() as cur:
            cur.execute('''DELETE FROM taldau.bronze_inv_snapshot_members WHERE snapshot_id=%s AND dimension='gsvziok'
                AND member_id=(SELECT min(gsvziok_id) FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=%s)''',
                (self.sid,self.sid))
        before=self.publication_image()
        with self.assertRaisesRegex(psycopg2.Error,'incomplete/invalid'):
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute('SELECT taldau.gold_publish_inv_snapshot(%s)',(self.sid,))
        self.assertEqual(self.publication_image(),before)
        with self.assertRaisesRegex(psycopg2.Error,'incomplete/invalid'):
            publish_snapshot(self.conn,self.sid)
        self.assertEqual(self.publication_image(),before)

    def test_gold_requires_published_silver_and_revalidates(self):
        self.complete()
        validate_snapshot(self.conn,self.sid)
        with self.assertRaisesRegex(psycopg2.Error,'must be published in Silver'):
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute('SELECT taldau.gold_publish_inv_snapshot(%s)',(self.sid,))
        publish_snapshot(self.conn,self.sid)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.staging_inv_year_cells SET value=NULL,raw_value='bad' WHERE snapshot_id=%s",(self.sid,))
        before=self.publication_image()
        with self.assertRaisesRegex(psycopg2.Error,'incomplete/invalid'):
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute('SELECT taldau.gold_publish_inv_snapshot(%s)',(self.sid,))
        self.assertEqual(self.publication_image(),before)

    def test_python_count_mismatch_rolls_back_both_layers(self):
        self.complete()
        validate_snapshot(self.conn,self.sid)
        before=self.publication_image()
        with self.conn.cursor() as cur:
            cur.execute('''CREATE OR REPLACE FUNCTION taldau.gold_publish_inv_snapshot(p_snapshot text)
                RETURNS bigint LANGUAGE sql AS 'SELECT 0::bigint' ''')
        with self.assertRaisesRegex(ValueError,'publication count mismatch'):
            publish_snapshot(self.conn,self.sid)
        self.assertEqual(self.publication_image(),before)

    def test_summary_of_incomplete_or_invalid_snapshot_is_not_ready(self):
        report=snapshot_summary(self.conn,self.sid)
        self.assertFalse(report['ready_to_publish'])
        self.assertEqual(report['chunks_queued'],2)
        self.assertEqual(report['numeric_rows'],0)
        self.complete()
        with self.conn.cursor() as cur:
            cur.execute('''UPDATE taldau.staging_inv_year_cells SET value=NULL,raw_value='unknown'
                WHERE snapshot_id=%s AND period_code='122025' ''',(self.sid,))
        with self.assertRaisesRegex(ValueError,'validation failed'):
            validate_snapshot(self.conn,self.sid)
        report=snapshot_summary(self.conn,self.sid)
        self.assertEqual(report['snapshot_state'],'failed')
        self.assertFalse(report['ready_to_publish'])
        self.assertGreater(report['invalid_rows'],0)
        self.assertGreater(report['blocking_checks_failed'],0)
        self.assertEqual(report['invalid_rows'],sum(m['invalid_rows'] for m in report['months']))

    def test_unfinished_snapshot_cannot_publish_or_delete_existing_silver(self):
        extract_chunk(self.conn,self.chunks[268012][0])
        with self.assertRaisesRegex(psycopg2.Error,'incomplete/invalid'):
            publish_snapshot(self.conn,self.sid)
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM taldau.silver_inv_fixed_assets WHERE source_run_id='astana-2025-12-pilot-v1'")
            self.assertEqual(cur.fetchone()[0],504)

    def test_resume_skips_completed_and_reclaims_dead_worker(self):
        astana=self.chunks[268012][0]
        country=self.chunks[741880][0]
        extract_chunk(self.conn,astana)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_inv_chunks SET state='running',lease_until=now()-interval '1 minute' WHERE chunk_id=%s",(country,))
        self.assertEqual(pending_chunk_ids(self.conn,self.sid),[country])
        self.assertTrue(extract_chunk(self.conn,astana)['already_complete'])
        self.assertEqual(extract_chunk(self.conn,country)['http_requests'],0)
        self.assertEqual(wave_outcome(self.conn,self.sid,[country]),'validate')

    def test_active_lease_blocks_resume(self):
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_inv_chunks SET state='running',lease_until=now()+interval '1 hour' WHERE chunk_id=%s",(self.chunks[268012][0],))
        with self.assertRaisesRegex(RuntimeError,'live lease'):
            pending_chunk_ids(self.conn,self.sid)

    def test_session_lock_prevents_stealing_a_live_worker(self):
        other=psycopg2.connect(self.raw_conn.dsn,password=os.getenv('PGPASSWORD'))
        try:
            with other.cursor() as cur:
                cur.execute('SELECT pg_advisory_lock(hashtextextended(%s,0))',(self.chunks[268012][1],))
            other.commit()
            with self.assertRaisesRegex(RuntimeError,'Active worker owns'):
                extract_chunk(self.conn,self.chunks[268012][0])
        finally:
            other.close()

    def test_wave_is_bounded_and_completed_chunks_are_not_remapped(self):
        with self.conn.cursor() as cur:
            cur.execute('''INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope)
                SELECT %s||':extra:'||n,'inv_fixed_assets_monthly',%s,'{}'::jsonb
                FROM generate_series(1,140) n''',(self.sid,Json(self.config)))
            cur.execute('''INSERT INTO taldau.bronze_inv_chunks(snapshot_id,territory_id,run_id)
                SELECT %s,9000000+n,%s||':extra:'||n FROM generate_series(1,140) n''',(self.sid,self.sid))
        self.assertEqual(len(pending_chunk_ids(self.conn,self.sid)),128)
        with self.assertRaisesRegex(ValueError,'Maximum wave size'):
            pending_chunk_ids(self.conn,self.sid,1025)

    def test_skipped_mapped_task_cannot_trigger_infinite_continuation(self):
        self.assertEqual(wave_outcome(self.conn,self.sid,[self.chunks[268012][0]]),'failed')

    def test_duplicate_and_null_keys_block_publication(self):
        self.complete()
        with self.conn.cursor() as cur:
            cur.execute('''UPDATE taldau.staging_inv_year_cells SET reporting_period=NULL WHERE snapshot_id=%s
                AND raw_id=(SELECT min(raw_id) FROM taldau.staging_inv_year_cells WHERE snapshot_id=%s)''',(self.sid,self.sid))
        result,errors=self.checks()
        self.assertFalse(result['valid'])
        self.assertGreater(errors['null_keys'],0)

    def test_duplicate_grain_not_silently_deduplicated(self):
        self.complete()
        with self.conn.cursor() as cur:
            cur.execute('''INSERT INTO taldau.staging_inv_year_cells
                SELECT snapshot_id,chunk_id,run_id,raw_id,node_ordinal+10000,indicator_id,
                    kato_id,krp_id,sif_id,gsvziok_id,period_code,reporting_period,reporting_period_text,
                    has_value,has_period,raw_value,value,value_measure
                FROM taldau.staging_inv_year_cells WHERE snapshot_id=%s LIMIT 1''',(self.sid,))
        result,errors=self.checks()
        self.assertFalse(result['valid'])
        self.assertEqual(errors['duplicate_natural_keys'],1)

    def test_unknown_value_and_orphan_pairs_block_publication(self):
        self.complete()
        with self.conn.cursor() as cur:
            cur.execute('''UPDATE taldau.staging_inv_year_cells SET raw_value='unexpected',value=NULL,has_period=false
                WHERE snapshot_id=%s AND period_code='122025' ''',(self.sid,))
        result,errors=self.checks()
        self.assertFalse(result['valid'])
        self.assertGreater(errors['unknown_non_numeric'],0)
        self.assertGreater(errors['orphan_period_value_pairs'],0)

    def test_hierarchy_conflict_blocks_publication(self):
        self.complete()
        with self.conn.cursor() as cur:
            cur.execute('''UPDATE taldau.bronze_inv_snapshot_members SET member_name='conflicting name'
                WHERE snapshot_id=%s AND raw_id=(SELECT min(raw_id) FROM taldau.bronze_inv_snapshot_members
                    WHERE snapshot_id=%s AND dimension='sif')''',(self.sid,self.sid))
        result,errors=self.checks()
        self.assertFalse(result['valid'])
        self.assertGreater(errors['conflicting_hierarchy'],0)

    def test_missing_request_checkpoint_detected_despite_complete_flag(self):
        self.complete()
        with self.conn.cursor() as cur:
            cur.execute('''DELETE FROM taldau.bronze_inv_request_tasks WHERE run_id=%s AND request_hash=
                (SELECT min(request_hash) FROM taldau.bronze_inv_request_tasks WHERE run_id=%s)''',
                (self.chunks[268012][1],self.chunks[268012][1]))
        result,errors=self.checks()
        self.assertFalse(result['valid'])
        self.assertGreater(errors['chunk_request_coverage'],0)

    def test_ets_full_join_mismatch_missing_extra_duplicates(self):
        self.complete()
        publish_snapshot(self.conn,self.sid)
        dataset='test_ets_'+uuid.uuid4().hex
        with self.conn.cursor() as cur:
            cur.execute('CREATE TEMP TABLE ets_fixture (ryear int,reporting_period bigint,kato1 bigint,krp bigint,sif bigint,gsvziok bigint,value numeric,space_element_set_id text) ON COMMIT DROP')
            cur.execute('''INSERT INTO ets_fixture SELECT 2025,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value,'fixture-only'
                FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=%s''',(self.sid,))
            cur.execute('SELECT taldau.reconciliation_capture_inv_ets(%s,%s::regclass)',(dataset,'ets_fixture'))
            # Controlled fixture IDs are known to be source IDs. Production requires reviewed evidence.
            for dimension,column in [('kato','kato1'),('krp','krp'),('sif','sif'),('gsvziok','gsvziok')]:
                cur.execute('''INSERT INTO taldau.reconciliation_ets_inv_key_map
                    SELECT DISTINCT %s,%s,source_row->>%s,(source_row->>%s)::bigint,'Synthetic fixture uses Taldau source IDs'
                    FROM taldau.reconciliation_ets_inv_raw WHERE dataset_id=%s''',(dataset,dimension,column,column,dataset))
            cur.execute('SELECT taldau.reconciliation_compare_inv_ets(%s,%s)',(self.sid,dataset))
            self.assertEqual(cur.fetchone()[0],0)
            cur.execute('''SELECT row_id FROM taldau.reconciliation_ets_inv_raw WHERE dataset_id=%s ORDER BY row_id LIMIT 3''',(dataset,))
            one,two,three=[r[0] for r in cur.fetchall()]
            cur.execute("UPDATE taldau.reconciliation_ets_inv_raw SET source_row=jsonb_set(source_row,'{value}','999') WHERE row_id=%s",(one,))
            cur.execute('DELETE FROM taldau.reconciliation_ets_inv_raw WHERE row_id=%s',(two,))
            cur.execute('INSERT INTO taldau.reconciliation_ets_inv_raw(dataset_id,source_row) SELECT dataset_id,source_row FROM taldau.reconciliation_ets_inv_raw WHERE row_id=%s',(three,))
            cur.execute("INSERT INTO taldau.reconciliation_ets_inv_key_map VALUES(%s,'gsvziok','999999999',999999999,'Synthetic extra ETS member')",(dataset,))
            cur.execute('''INSERT INTO taldau.reconciliation_ets_inv_raw(dataset_id,source_row)
                SELECT dataset_id,jsonb_set(source_row,'{gsvziok}','999999999')
                FROM taldau.reconciliation_ets_inv_raw WHERE row_id=%s''',(one,))
            cur.execute('SELECT taldau.reconciliation_compare_inv_ets(%s,%s)',(self.sid,dataset))
            self.assertEqual(cur.fetchone()[0],4)
            cur.execute('''SELECT sum(missing_in_taldau),sum(extra_in_taldau),sum(value_mismatch),sum(duplicates_ets)
                FROM taldau.reconciliation_inv_results WHERE snapshot_id=%s AND dataset_id=%s''',(self.sid,dataset))
            self.assertEqual(cur.fetchone(),(1,1,1,1))


if __name__=='__main__': unittest.main()
