"""Single-schema baseline and legacy migration checks. No network or production access."""
from __future__ import annotations

import os
from pathlib import Path
import re
import unittest

import psycopg2

ROOT=Path(__file__).resolve().parents[1]
SQL_DIR=ROOT/'dags/taldau_elt/sql'
BASELINE=[f'{number:03d}_{name}.sql' for number,name in (
    (1,'bronze'),(2,'silver'),(3,'gold'),(4,'snapshot_model'),
    (5,'snapshot_validation'),(6,'ets_reconciliation'),
    (7,'gold_snapshot_publish'),(8,'multi_year_snapshot'))]

LEGACY_TO_NEW={
    'metadata.elt_pipelines':'taldau.metadata_elt_pipelines',
    'metadata.taldau_indicators':'taldau.metadata_taldau_indicators',
    'bronze.extraction_runs':'taldau.bronze_extraction_runs',
    'bronze.taldau_api_raw':'taldau.bronze_taldau_api_raw',
    'bronze.v_taldau_tree_nodes':'taldau.bronze_v_taldau_tree_nodes',
    'bronze.v_inv_candidates':'taldau.bronze_v_inv_candidates',
    'bronze.validate_inv_pilot':'taldau.bronze_validate_inv_pilot',
    'bronze.inv_snapshots':'taldau.bronze_inv_snapshots',
    'bronze.inv_chunks':'taldau.bronze_inv_chunks',
    'bronze.inv_request_tasks':'taldau.bronze_inv_request_tasks',
    'bronze.inv_snapshot_members':'taldau.bronze_inv_snapshot_members',
    'bronze.inv_reuse_raw':'taldau.bronze_inv_reuse_raw',
    'bronze.inv_run_raw':'taldau.bronze_inv_run_raw',
    'bronze.inv_int':'taldau.bronze_inv_int',
    'bronze.stage_inv_run':'taldau.bronze_stage_inv_run',
    'silver.dim_territory':'taldau.silver_dim_territory',
    'silver.inv_fixed_assets':'taldau.silver_inv_fixed_assets',
    'silver.refresh_inv_pilot':'taldau.silver_refresh_inv_pilot',
    'silver.publish_inv_snapshot':'taldau.silver_publish_inv_snapshot',
    'silver.statistics_region':'taldau.silver_statistics_region',
    'gold.dim_inv_member':'taldau.gold_dim_inv_member',
    'gold.dim_inv_period':'taldau.gold_dim_inv_period',
    'gold.fact_inv_fixed_assets':'taldau.gold_fact_inv_fixed_assets',
    'gold.v_inv_fixed_assets':'taldau.gold_v_inv_fixed_assets',
    'gold.mart_inv_territory_totals':'taldau.gold_mart_inv_territory_totals',
    'gold.refresh_inv_pilot':'taldau.gold_refresh_inv_pilot',
    'gold.publish_inv_snapshot':'taldau.gold_publish_inv_snapshot',
    'gold.dim_region':'taldau.gold_dim_region',
    'gold.dim_indicator':'taldau.gold_dim_indicator',
    'gold.dim_date':'taldau.gold_dim_date',
    'gold.fact_statistics':'taldau.gold_fact_statistics',
    'gold.v_region_year_metrics':'taldau.gold_v_region_year_metrics',
    'staging.inv_year_cells':'taldau.staging_inv_year_cells',
    'staging.stage_inv_chunk':'taldau.staging_stage_inv_chunk',
    'quality.inv_month_expectations':'taldau.quality_inv_month_expectations',
    'quality.inv_snapshot_checks':'taldau.quality_inv_snapshot_checks',
    'quality.inv_month_diagnostics':'taldau.quality_inv_month_diagnostics',
    'quality.inv_run_coverage':'taldau.quality_inv_run_coverage',
    'quality.inv_discovery_errors':'taldau.quality_inv_discovery_errors',
    'quality.validate_inv_snapshot':'taldau.quality_validate_inv_snapshot',
    'quality.inv_year_coverage':'taldau.quality_inv_year_coverage',
    'reconciliation.capture_inv_ets':'taldau.reconciliation_capture_inv_ets',
    'reconciliation.compare_inv_ets':'taldau.reconciliation_compare_inv_ets',
    'reconciliation.ets_inv_datasets':'taldau.reconciliation_ets_inv_datasets',
    'reconciliation.ets_inv_key_map':'taldau.reconciliation_ets_inv_key_map',
    'reconciliation.ets_inv_raw':'taldau.reconciliation_ets_inv_raw',
    'reconciliation.inv_differences':'taldau.reconciliation_inv_differences',
    'reconciliation.inv_results':'taldau.reconciliation_inv_results',
    'reconciliation.v_ets_inv_resolved':'taldau.reconciliation_v_ets_inv_resolved',
}
NAME_REPLACEMENTS={
    'taldau_raw_run_dimension_idx':'bronze_taldau_raw_run_dimension_idx',
    'inv_fixed_assets_run_idx':'silver_inv_fixed_assets_run_idx',
    'inv_chunks_pending_idx':'bronze_inv_chunks_pending_idx',
    'inv_members_lookup_idx':'bronze_inv_members_lookup_idx',
    'inv_members_run_idx':'bronze_inv_members_run_idx',
    'inv_cells_chunk_idx':'staging_inv_cells_chunk_idx',
    'inv_cells_grain_idx':'staging_inv_cells_grain_idx',
    'inv_silver_snapshot_idx':'silver_inv_snapshot_idx',
    'ets_inv_dataset_idx':'reconciliation_ets_inv_dataset_idx',
    'inv_reuse_raw_id_idx':'bronze_inv_reuse_raw_id_idx',
    'inv_snapshots_year_check':'bronze_inv_snapshots_year_check',
    'inv_snapshot_range_check':'bronze_inv_snapshot_range_check',
}
SCHEMA_DDL={
    '001_bronze.sql':'CREATE SCHEMA IF NOT EXISTS bronze; CREATE SCHEMA IF NOT EXISTS metadata;',
    '002_silver.sql':'CREATE SCHEMA IF NOT EXISTS silver;',
    '003_gold.sql':'CREATE SCHEMA IF NOT EXISTS gold;',
    '004_snapshot_model.sql':'CREATE SCHEMA IF NOT EXISTS staging; CREATE SCHEMA IF NOT EXISTS quality;',
    '006_ets_reconciliation.sql':'CREATE SCHEMA IF NOT EXISTS reconciliation;',
}
LEGACY_PATTERN=re.compile(r'\b(?:bronze|silver|gold|staging|quality|reconciliation|metadata)\.[A-Za-z_][A-Za-z0-9_]*')
FUNCTION_TARGETS={
    'taldau.bronze_inv_int','taldau.bronze_stage_inv_run','taldau.bronze_validate_inv_pilot',
    'taldau.staging_stage_inv_chunk','taldau.quality_inv_run_coverage',
    'taldau.quality_inv_discovery_errors','taldau.quality_validate_inv_snapshot',
    'taldau.silver_refresh_inv_pilot','taldau.silver_publish_inv_snapshot',
    'taldau.gold_refresh_inv_pilot','taldau.gold_publish_inv_snapshot',
    'taldau.reconciliation_capture_inv_ets','taldau.reconciliation_compare_inv_ets',
}


def connection(database=None):
    return psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=os.getenv('PGPORT','5434'),
        dbname=database or os.getenv('PGDATABASE','taldau'),user=os.getenv('PGUSER','taldau'),
        password=os.getenv('PGPASSWORD'))


def legacy_sql(name):
    text=(SQL_DIR/name).read_text(encoding='utf-8')
    text=text.replace('CREATE SCHEMA IF NOT EXISTS taldau;',SCHEMA_DDL.get(name,''))
    for old,new in sorted(LEGACY_TO_NEW.items(),key=lambda pair:len(pair[1]),reverse=True):
        text=text.replace(new,old)
    for old,new in NAME_REPLACEMENTS.items():
        text=text.replace(new,old)
    return text


class StaticSingleSchemaTests(unittest.TestCase):
    def test_runtime_and_baseline_have_no_legacy_project_qualifiers(self):
        paths=list((ROOT/'dags').rglob('*.py'))+list((ROOT/'tools').rglob('*.py'))
        paths += [SQL_DIR/name for name in BASELINE]
        for path in paths:
            matches=LEGACY_PATTERN.findall(path.read_text(encoding='utf-8'))
            self.assertEqual(matches,[],f'{path}: {matches[:5]}')

    def test_baseline_creates_only_taldau_schema(self):
        created=[]
        for name in BASELINE:
            created += re.findall(r'CREATE SCHEMA IF NOT EXISTS\s+([a-z_]+)',
                                  (SQL_DIR/name).read_text(encoding='utf-8'),re.I)
        self.assertTrue(created)
        self.assertEqual(set(map(str.lower,created)),{'taldau'})

    def test_compatibility_migration_never_drops_shared_schemas(self):
        sql=(SQL_DIR/'009_single_taldau_schema.sql').read_text(encoding='utf-8')
        self.assertNotRegex(sql,r'(?i)DROP\s+SCHEMA')
        self.assertIn('CREATE SCHEMA IF NOT EXISTS taldau',sql)


@unittest.skipUnless(os.getenv('TALDAU_TEST_DB')=='1','Requires isolated test DB')
class FreshSingleSchemaTests(unittest.TestCase):
    def test_fresh_install_places_project_objects_in_taldau(self):
        conn=connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE c.relname IN ('extraction_runs','taldau_api_raw','inv_snapshots','inv_chunks',
                        'inv_fixed_assets','fact_inv_fixed_assets','ets_inv_datasets')""")
                self.assertEqual(cur.fetchall(),[])
                cur.execute("""SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname='taldau' AND c.relname IN ('bronze_extraction_runs',
                        'bronze_taldau_api_raw','bronze_inv_snapshots','bronze_inv_chunks',
                        'silver_inv_fixed_assets','gold_fact_inv_fixed_assets',
                        'reconciliation_ets_inv_datasets')""")
                self.assertEqual(cur.fetchone()[0],7)
        finally:
            conn.close()


@unittest.skipUnless(os.getenv('TALDAU_LEGACY_TEST_DATABASE'),'Requires isolated legacy test DB')
class LegacyMigrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn=connection(os.environ['TALDAU_LEGACY_TEST_DATABASE'])
        with cls.conn:
            with cls.conn.cursor() as cur:
                for name in BASELINE:
                    cur.execute(legacy_sql(name))
                cur.execute("""
                    CREATE TABLE metadata.taldau_indicators(indicator_id bigint PRIMARY KEY);
                    CREATE TABLE silver.statistics_region(indicator_id bigint,region_id bigint,period_date date,value numeric);
                    CREATE TABLE gold.dim_region(region_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,source_region_id bigint UNIQUE);
                    CREATE TABLE gold.dim_indicator(indicator_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,source_indicator_id bigint UNIQUE);
                    CREATE TABLE gold.dim_date(date_key integer PRIMARY KEY,full_date date UNIQUE);
                    CREATE TABLE gold.fact_statistics(indicator_key bigint REFERENCES gold.dim_indicator,
                        region_key bigint REFERENCES gold.dim_region,date_key integer REFERENCES gold.dim_date,
                        PRIMARY KEY(indicator_key,region_key,date_key));
                    CREATE VIEW gold.v_region_year_metrics AS SELECT source_indicator_id FROM gold.dim_indicator;
                """)
                for schema in ('metadata','bronze','silver','gold','staging','quality','reconciliation'):
                    cur.execute(f'CREATE TABLE {schema}.smart_aqmola_keep(id integer PRIMARY KEY)')
                    cur.execute(f'INSERT INTO {schema}.smart_aqmola_keep VALUES(1)')
                cur.execute('CREATE TABLE public.smart_aqmola_keep(id integer PRIMARY KEY)')
                cur.execute('INSERT INTO public.smart_aqmola_keep VALUES(1)')
                cur.execute("""INSERT INTO bronze.extraction_runs(run_id,pipeline_id,config,scope,status)
                    VALUES('legacy-preserved','inv_fixed_assets_monthly','{}','{}','bronze_complete')""")
                cur.execute("""INSERT INTO bronze.inv_snapshots
                    (snapshot_id,year,config,discovery_run_id,state,year_start,year_end)
                    VALUES('legacy-preserved',2025,'{}','legacy-preserved','prepared',2025,2025)""")
                migration=(SQL_DIR/'009_single_taldau_schema.sql').read_text(encoding='utf-8')
                cur.execute(migration)
                cur.execute(migration)  # repeat must be a no-op

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_data_dependencies_and_function_bodies_are_preserved(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM taldau.bronze_inv_snapshots WHERE snapshot_id='legacy-preserved'")
            self.assertEqual(cur.fetchone()[0],1)
            cur.execute("SELECT taldau.bronze_inv_int('123')")
            self.assertEqual(cur.fetchone()[0],123)
            cur.execute("""SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='taldau' AND p.proname LIKE ANY(ARRAY['bronze_%','silver_%','gold_%',
                    'staging_%','quality_%','reconciliation_%'])""")
            definitions='\n'.join(row[0] for row in cur.fetchall())
            self.assertIsNone(LEGACY_PATTERN.search(definitions),definitions)
            cur.execute("""SELECT count(*) FROM pg_constraint con
                WHERE con.conrelid='taldau.bronze_inv_snapshots'::regclass
                  AND con.confrelid='taldau.bronze_extraction_runs'::regclass""")
            self.assertGreater(cur.fetchone()[0],0)

    def test_only_whitelisted_objects_moved_and_shared_schemas_remain(self):
        with self.conn.cursor() as cur:
            for schema in ('metadata','bronze','silver','gold','staging','quality','reconciliation','public'):
                cur.execute(f'SELECT count(*) FROM {schema}.smart_aqmola_keep')
                self.assertEqual(cur.fetchone()[0],1,schema)
                cur.execute('SELECT to_regnamespace(%s) IS NOT NULL',(schema,))
                self.assertTrue(cur.fetchone()[0],schema)
            for old in LEGACY_TO_NEW:
                cur.execute('SELECT to_regclass(%s)',(old,))
                self.assertIsNone(cur.fetchone()[0],old)
            for new in LEGACY_TO_NEW.values():
                if new in FUNCTION_TARGETS:
                    continue
                cur.execute('SELECT to_regclass(%s)',(new,))
                self.assertIsNotNone(cur.fetchone()[0],new)


if __name__=='__main__':
    unittest.main()
