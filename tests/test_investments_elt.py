"""Unit + opt-in transactional integration tests. All test DB writes are rolled back."""
import json
import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock

import psycopg2

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'dags'))
from taldau_elt.loader import BronzeLoader,PILOT_SCOPE,request_hash


class TraversalTests(unittest.TestCase):
    def test_hash_is_order_independent_but_captures_parameters(self):
        self.assertEqual(request_hash('url',{'b':'2','a':'1'}),request_hash('url',{'a':'1','b':'2'}))
        self.assertNotEqual(request_hash('url',{'a':'1'}),request_hash('url',{'a':'2'}))

    def test_parent_values_and_x_are_not_filtered(self):
        loader=BronzeLoader(Mock(),'test',{},PILOT_SCOPE)
        parent={'id':'1','text':'parent','leaf':False,'y122025':'1798175695000'}
        child={'id':'2','text':'child','leaf':'true','y122025':'x'}
        loader.fetch=Mock(side_effect=[[parent],[child]])
        self.assertEqual(list(loader.walk([], 'gsvziok')),[parent,child])
        loader.session.close()

    def test_cycle_fails_instead_of_silently_discarding_nodes(self):
        loader=BronzeLoader(Mock(),'test',{},PILOT_SCOPE)
        node={'id':'1','leaf':'false'}
        loader.fetch=Mock(side_effect=[[node],[node]])
        with self.assertRaisesRegex(ValueError,'Duplicate/cyclic'):
            list(loader.walk([], 'gsvziok'))
        loader.session.close()


class NoCommit:
    """Keep loader transactions inside the test's outer rollback transaction."""
    def __init__(self,conn): self.conn=conn
    def cursor(self): return self.conn.cursor()
    def commit(self): pass
    def rollback(self): self.conn.rollback()
    def __enter__(self): return self
    def __exit__(self,*args): return False


@unittest.skipUnless(os.getenv('TALDAU_TEST_DB')=='1','Set TALDAU_TEST_DB=1 for local transactional checks')
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn=psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=os.getenv('PGPORT','5434'),
            dbname=os.getenv('PGDATABASE','taldau'),user=os.getenv('PGUSER','taldau'),
            password=os.getenv('PGPASSWORD'))
        self.run='test-'+uuid.uuid4().hex
        with self.conn.cursor() as cur:
            cur.execute('''INSERT INTO taldau.bronze_extraction_runs(run_id,pipeline_id,config,scope,status)
                SELECT %s,pipeline_id,config,scope,'bronze_complete' FROM taldau.bronze_extraction_runs
                WHERE run_id='astana-2025-12-pilot-v1' RETURNING config''',(self.run,))
            self.config=cur.fetchone()[0]
            cur.execute('''INSERT INTO taldau.bronze_taldau_api_raw
                (run_id,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                 response_text,response_hash,http_status,dimension,tree_depth)
                SELECT %s,indicator_id,endpoint,period_id,request_params,request_hash,response_data,
                    response_text,response_hash,http_status,dimension,tree_depth
                FROM taldau.bronze_taldau_api_raw WHERE run_id='astana-2025-12-pilot-v1' ''',(self.run,))

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def test_partial_run_resumes_entire_tree_without_network(self):
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_extraction_runs SET status='failed' WHERE run_id=%s",(self.run,))
        loader=BronzeLoader(NoCommit(self.conn),self.run,self.config,PILOT_SCOPE,delay=0)
        loader.session.get=Mock(side_effect=AssertionError('Resume must use saved responses'))
        result=loader.extract_pilot()
        self.assertEqual(result['http_requests'],0)
        self.assertEqual(result['cache_hits'],414)
        loader.session.get.assert_not_called()

    def _change_node(self,change):
        with self.conn.cursor() as cur:
            cur.execute('''SELECT id,response_text FROM taldau.bronze_taldau_api_raw
                WHERE run_id=%s AND dimension='gsvziok'
                  AND request_params->>'p_terms'='268012,741927,807855,19202525'
                  AND response_data @> '[{"id":"19202537"}]'::jsonb''',(self.run,))
            raw_id,text=cur.fetchone()
            nodes=json.loads(text)
            change(nodes,next(n for n in nodes if n['id']=='19202537'))
            body=json.dumps(nodes,ensure_ascii=False)
            cur.execute('UPDATE taldau.bronze_taldau_api_raw SET response_data=%s::jsonb,response_text=%s WHERE id=%s',
                        (body,body,raw_id))

    def test_unknown_value_fails_quality_gate(self):
        self._change_node(lambda nodes,node:node.update(y122025='unexpected'))
        with self.assertRaisesRegex(psycopg2.Error,'Invalid rows'):
            with self.conn.cursor() as cur: cur.execute('SELECT taldau.bronze_validate_inv_pilot(%s)',(self.run,))

    def test_duplicate_combination_is_not_deduplicated_silently(self):
        self._change_node(lambda nodes,node:nodes.append(node.copy()))
        with self.assertRaisesRegex(psycopg2.Error,'duplicate natural keys: 1'):
            with self.conn.cursor() as cur: cur.execute('SELECT taldau.bronze_validate_inv_pilot(%s)',(self.run,))

    def test_x_change_is_preserved_but_does_not_fake_expected_counts(self):
        self._change_node(lambda nodes,node:node.update(y122025='x'))
        with self.assertRaisesRegex(psycopg2.Error,'numeric 503, x 8'):
            with self.conn.cursor() as cur: cur.execute('SELECT taldau.silver_refresh_inv_pilot(%s)',(self.run,))

    def test_exact_decimal_and_offline_sql_rebuild(self):
        self._change_node(lambda nodes,node:node.update(y122025='1798175695000.123456789'))
        with self.conn.cursor() as cur:
            cur.execute('SELECT taldau.silver_refresh_inv_pilot(%s)',(self.run,))
            cur.execute('''SELECT value::text FROM taldau.silver_inv_fixed_assets WHERE source_run_id=%s
                AND krp_id=741927 AND sif_id=807855 AND gsvziok_id=19202537''',(self.run,))
            self.assertEqual(cur.fetchone()[0],'1798175695000.123456789')
            cur.execute('SELECT taldau.gold_refresh_inv_pilot(%s)',(self.run,))
            self.assertEqual(cur.fetchone()[0],504)

    def test_incomplete_run_cannot_publish_silver(self):
        with self.conn.cursor() as cur:
            cur.execute("UPDATE taldau.bronze_extraction_runs SET status='loading' WHERE run_id=%s",(self.run,))
        with self.assertRaisesRegex(psycopg2.Error,'Extraction is not complete'):
            with self.conn.cursor() as cur: cur.execute('SELECT taldau.silver_refresh_inv_pilot(%s)',(self.run,))


if __name__=='__main__': unittest.main()
