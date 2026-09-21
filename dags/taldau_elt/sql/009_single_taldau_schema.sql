-- Compatibility migration for installations that already applied legacy migrations 001-008.
-- Run in one transaction. Only explicitly listed Taldau ELT objects are moved; shared schemas
-- and unrelated objects are never dropped or altered.
CREATE SCHEMA IF NOT EXISTS taldau;

DO $migration$
DECLARE
    item record;
    dependency record;
    source_oid oid;
    target_oid oid;
    source_kind "char";
    function_definition text;
    desired_name text;
    sequence_name text;
    sequence_oid oid;
BEGIN
    -- Tables first. ALTER TABLE SET SCHEMA retains data, indexes, constraints, FKs and OIDs.
    FOR item IN SELECT * FROM (VALUES
        ('metadata','elt_pipelines','metadata_elt_pipelines'),
        ('metadata','taldau_indicators','metadata_taldau_indicators'),
        ('bronze','extraction_runs','bronze_extraction_runs'),
        ('bronze','taldau_api_raw','bronze_taldau_api_raw'),
        ('bronze','inv_snapshots','bronze_inv_snapshots'),
        ('bronze','inv_chunks','bronze_inv_chunks'),
        ('bronze','inv_request_tasks','bronze_inv_request_tasks'),
        ('bronze','inv_snapshot_members','bronze_inv_snapshot_members'),
        ('bronze','inv_reuse_raw','bronze_inv_reuse_raw'),
        ('silver','dim_territory','silver_dim_territory'),
        ('silver','inv_fixed_assets','silver_inv_fixed_assets'),
        ('silver','statistics_region','silver_statistics_region'),
        ('staging','inv_year_cells','staging_inv_year_cells'),
        ('quality','inv_month_expectations','quality_inv_month_expectations'),
        ('quality','inv_snapshot_checks','quality_inv_snapshot_checks'),
        ('quality','inv_month_diagnostics','quality_inv_month_diagnostics'),
        ('reconciliation','ets_inv_datasets','reconciliation_ets_inv_datasets'),
        ('reconciliation','ets_inv_raw','reconciliation_ets_inv_raw'),
        ('reconciliation','ets_inv_key_map','reconciliation_ets_inv_key_map'),
        ('reconciliation','inv_results','reconciliation_inv_results'),
        ('reconciliation','inv_differences','reconciliation_inv_differences'),
        ('gold','dim_inv_member','gold_dim_inv_member'),
        ('gold','dim_inv_period','gold_dim_inv_period'),
        ('gold','fact_inv_fixed_assets','gold_fact_inv_fixed_assets'),
        ('gold','dim_region','gold_dim_region'),
        ('gold','dim_indicator','gold_dim_indicator'),
        ('gold','dim_date','gold_dim_date'),
        ('gold','fact_statistics','gold_fact_statistics')
    ) AS objects(old_schema,old_name,new_name)
    LOOP
        source_oid := to_regclass(format('%I.%I',item.old_schema,item.old_name));
        target_oid := to_regclass(format('taldau.%I',item.new_name));
        IF source_oid IS NOT NULL AND target_oid IS NOT NULL THEN
            RAISE EXCEPTION 'Both legacy %.% and target taldau.% exist; refusing to merge',
                item.old_schema,item.old_name,item.new_name;
        ELSIF source_oid IS NOT NULL THEN
            SELECT relkind INTO source_kind FROM pg_class WHERE oid=source_oid;
            IF source_kind NOT IN ('r','p') THEN
                RAISE EXCEPTION 'Legacy %.% has unexpected relkind %',
                    item.old_schema,item.old_name,source_kind;
            END IF;
            EXECUTE format('ALTER TABLE %I.%I SET SCHEMA taldau',item.old_schema,item.old_name);
            EXECUTE format('ALTER TABLE taldau.%I RENAME TO %I',item.old_name,item.new_name);
        END IF;
    END LOOP;

    -- Views keep their dependency graph because they are moved and renamed, not recreated/dropped.
    FOR item IN SELECT * FROM (VALUES
        ('bronze','v_taldau_tree_nodes','bronze_v_taldau_tree_nodes'),
        ('bronze','v_inv_candidates','bronze_v_inv_candidates'),
        ('bronze','inv_run_raw','bronze_inv_run_raw'),
        ('quality','inv_year_coverage','quality_inv_year_coverage'),
        ('reconciliation','v_ets_inv_resolved','reconciliation_v_ets_inv_resolved'),
        ('gold','v_inv_fixed_assets','gold_v_inv_fixed_assets'),
        ('gold','mart_inv_territory_totals','gold_mart_inv_territory_totals'),
        ('gold','v_region_year_metrics','gold_v_region_year_metrics')
    ) AS objects(old_schema,old_name,new_name)
    LOOP
        source_oid := to_regclass(format('%I.%I',item.old_schema,item.old_name));
        target_oid := to_regclass(format('taldau.%I',item.new_name));
        IF source_oid IS NOT NULL AND target_oid IS NOT NULL THEN
            RAISE EXCEPTION 'Both legacy %.% and target taldau.% exist; refusing to merge',
                item.old_schema,item.old_name,item.new_name;
        ELSIF source_oid IS NOT NULL THEN
            SELECT relkind INTO source_kind FROM pg_class WHERE oid=source_oid;
            IF source_kind='v' THEN
                EXECUTE format('ALTER VIEW %I.%I SET SCHEMA taldau',item.old_schema,item.old_name);
                EXECUTE format('ALTER VIEW taldau.%I RENAME TO %I',item.old_name,item.new_name);
            ELSIF source_kind='m' THEN
                EXECUTE format('ALTER MATERIALIZED VIEW %I.%I SET SCHEMA taldau',item.old_schema,item.old_name);
                EXECUTE format('ALTER MATERIALIZED VIEW taldau.%I RENAME TO %I',item.old_name,item.new_name);
            ELSE
                RAISE EXCEPTION 'Legacy %.% has unexpected relkind %',
                    item.old_schema,item.old_name,source_kind;
            END IF;
        END IF;
    END LOOP;

    -- Move functions without dropping them so dependent objects retain the same function OIDs.
    FOR item IN SELECT * FROM (VALUES
        ('bronze','inv_int','text','bronze_inv_int'),
        ('bronze','stage_inv_run','text, text','bronze_stage_inv_run'),
        ('bronze','validate_inv_pilot','text','bronze_validate_inv_pilot'),
        ('staging','stage_inv_chunk','bigint','staging_stage_inv_chunk'),
        ('quality','inv_run_coverage','text','quality_inv_run_coverage'),
        ('quality','inv_discovery_errors','text','quality_inv_discovery_errors'),
        ('quality','validate_inv_snapshot','text','quality_validate_inv_snapshot'),
        ('silver','refresh_inv_pilot','text','silver_refresh_inv_pilot'),
        ('silver','publish_inv_snapshot','text','silver_publish_inv_snapshot'),
        ('gold','refresh_inv_pilot','text','gold_refresh_inv_pilot'),
        ('gold','publish_inv_snapshot','text','gold_publish_inv_snapshot'),
        ('reconciliation','capture_inv_ets','text, regclass, integer','reconciliation_capture_inv_ets'),
        ('reconciliation','compare_inv_ets','text, text','reconciliation_compare_inv_ets')
    ) AS functions(old_schema,old_name,identity_args,new_name)
    LOOP
        source_oid := to_regprocedure(format('%I.%I(%s)',item.old_schema,item.old_name,item.identity_args));
        target_oid := to_regprocedure(format('taldau.%I(%s)',item.new_name,item.identity_args));
        IF source_oid IS NOT NULL AND target_oid IS NOT NULL THEN
            RAISE EXCEPTION 'Both legacy %.%(%) and target taldau.%(%) exist; refusing to merge',
                item.old_schema,item.old_name,item.identity_args,item.new_name,item.identity_args;
        ELSIF source_oid IS NOT NULL THEN
            EXECUTE format('ALTER FUNCTION %I.%I(%s) SET SCHEMA taldau',
                item.old_schema,item.old_name,item.identity_args);
            EXECUTE format('ALTER FUNCTION taldau.%I(%s) RENAME TO %I',
                item.old_name,item.identity_args,item.new_name);
        END IF;
    END LOOP;

    -- Recompile only the whitelisted functions, replacing legacy qualified references in bodies.
    FOR item IN SELECT * FROM (VALUES
        ('bronze_inv_int','text'),
        ('bronze_stage_inv_run','text, text'),
        ('bronze_validate_inv_pilot','text'),
        ('staging_stage_inv_chunk','bigint'),
        ('quality_inv_run_coverage','text'),
        ('quality_inv_discovery_errors','text'),
        ('quality_validate_inv_snapshot','text'),
        ('silver_refresh_inv_pilot','text'),
        ('silver_publish_inv_snapshot','text'),
        ('gold_refresh_inv_pilot','text'),
        ('gold_publish_inv_snapshot','text'),
        ('reconciliation_capture_inv_ets','text, regclass, integer'),
        ('reconciliation_compare_inv_ets','text, text')
    ) AS functions(new_name,identity_args)
    LOOP
        target_oid := to_regprocedure(format('taldau.%I(%s)',item.new_name,item.identity_args));
        IF target_oid IS NOT NULL THEN
            function_definition := pg_get_functiondef(target_oid);
            FOR dependency IN SELECT * FROM (VALUES
                ('metadata.elt_pipelines','taldau.metadata_elt_pipelines'),
                ('metadata.taldau_indicators','taldau.metadata_taldau_indicators'),
                ('bronze.extraction_runs','taldau.bronze_extraction_runs'),
                ('bronze.taldau_api_raw','taldau.bronze_taldau_api_raw'),
                ('bronze.v_taldau_tree_nodes','taldau.bronze_v_taldau_tree_nodes'),
                ('bronze.v_inv_candidates','taldau.bronze_v_inv_candidates'),
                ('bronze.inv_snapshots','taldau.bronze_inv_snapshots'),
                ('bronze.inv_chunks','taldau.bronze_inv_chunks'),
                ('bronze.inv_request_tasks','taldau.bronze_inv_request_tasks'),
                ('bronze.inv_snapshot_members','taldau.bronze_inv_snapshot_members'),
                ('bronze.inv_reuse_raw','taldau.bronze_inv_reuse_raw'),
                ('bronze.inv_run_raw','taldau.bronze_inv_run_raw'),
                ('bronze.inv_int','taldau.bronze_inv_int'),
                ('bronze.stage_inv_run','taldau.bronze_stage_inv_run'),
                ('bronze.validate_inv_pilot','taldau.bronze_validate_inv_pilot'),
                ('silver.dim_territory','taldau.silver_dim_territory'),
                ('silver.inv_fixed_assets','taldau.silver_inv_fixed_assets'),
                ('silver.refresh_inv_pilot','taldau.silver_refresh_inv_pilot'),
                ('silver.publish_inv_snapshot','taldau.silver_publish_inv_snapshot'),
                ('silver.statistics_region','taldau.silver_statistics_region'),
                ('gold.dim_inv_member','taldau.gold_dim_inv_member'),
                ('gold.dim_inv_period','taldau.gold_dim_inv_period'),
                ('gold.fact_inv_fixed_assets','taldau.gold_fact_inv_fixed_assets'),
                ('gold.v_inv_fixed_assets','taldau.gold_v_inv_fixed_assets'),
                ('gold.mart_inv_territory_totals','taldau.gold_mart_inv_territory_totals'),
                ('gold.refresh_inv_pilot','taldau.gold_refresh_inv_pilot'),
                ('gold.publish_inv_snapshot','taldau.gold_publish_inv_snapshot'),
                ('staging.inv_year_cells','taldau.staging_inv_year_cells'),
                ('staging.stage_inv_chunk','taldau.staging_stage_inv_chunk'),
                ('quality.inv_month_expectations','taldau.quality_inv_month_expectations'),
                ('quality.inv_snapshot_checks','taldau.quality_inv_snapshot_checks'),
                ('quality.inv_month_diagnostics','taldau.quality_inv_month_diagnostics'),
                ('quality.inv_run_coverage','taldau.quality_inv_run_coverage'),
                ('quality.inv_discovery_errors','taldau.quality_inv_discovery_errors'),
                ('quality.validate_inv_snapshot','taldau.quality_validate_inv_snapshot'),
                ('quality.inv_year_coverage','taldau.quality_inv_year_coverage'),
                ('reconciliation.capture_inv_ets','taldau.reconciliation_capture_inv_ets'),
                ('reconciliation.compare_inv_ets','taldau.reconciliation_compare_inv_ets'),
                ('reconciliation.ets_inv_datasets','taldau.reconciliation_ets_inv_datasets'),
                ('reconciliation.ets_inv_key_map','taldau.reconciliation_ets_inv_key_map'),
                ('reconciliation.ets_inv_raw','taldau.reconciliation_ets_inv_raw'),
                ('reconciliation.inv_differences','taldau.reconciliation_inv_differences'),
                ('reconciliation.inv_results','taldau.reconciliation_inv_results'),
                ('reconciliation.v_ets_inv_resolved','taldau.reconciliation_v_ets_inv_resolved')
            ) AS names(old_qualified,new_qualified)
            LOOP
                function_definition := replace(function_definition,
                    dependency.old_qualified,dependency.new_qualified);
            END LOOP;
            EXECUTE function_definition;
        END IF;
    END LOOP;

    -- Normalize explicit non-constraint index names after their owning tables moved.
    FOR item IN SELECT * FROM (VALUES
        ('taldau_raw_run_dimension_idx','bronze_taldau_raw_run_dimension_idx'),
        ('inv_fixed_assets_run_idx','silver_inv_fixed_assets_run_idx'),
        ('inv_chunks_pending_idx','bronze_inv_chunks_pending_idx'),
        ('inv_members_lookup_idx','bronze_inv_members_lookup_idx'),
        ('inv_members_run_idx','bronze_inv_members_run_idx'),
        ('inv_cells_chunk_idx','staging_inv_cells_chunk_idx'),
        ('inv_cells_grain_idx','staging_inv_cells_grain_idx'),
        ('inv_silver_snapshot_idx','silver_inv_snapshot_idx'),
        ('ets_inv_dataset_idx','reconciliation_ets_inv_dataset_idx'),
        ('inv_reuse_raw_id_idx','bronze_inv_reuse_raw_id_idx')
    ) AS indexes(old_name,new_name)
    LOOP
        source_oid := to_regclass(format('taldau.%I',item.old_name));
        target_oid := to_regclass(format('taldau.%I',item.new_name));
        IF source_oid IS NOT NULL AND target_oid IS NOT NULL THEN
            RAISE EXCEPTION 'Both legacy and target index names exist: %, %',item.old_name,item.new_name;
        ELSIF source_oid IS NOT NULL THEN
            EXECUTE format('ALTER INDEX taldau.%I RENAME TO %I',item.old_name,item.new_name);
        END IF;
    END LOOP;

    -- Identity sequences follow their tables to taldau, but PostgreSQL does not rename them.
    FOR item IN SELECT * FROM (VALUES
        ('bronze_taldau_api_raw','id','bronze_taldau_api_raw_id_seq'),
        ('bronze_inv_chunks','chunk_id','bronze_inv_chunks_chunk_id_seq'),
        ('reconciliation_ets_inv_raw','row_id','reconciliation_ets_inv_raw_row_id_seq'),
        ('gold_dim_inv_member','member_key','gold_dim_inv_member_member_key_seq'),
        ('gold_dim_region','region_key','gold_dim_region_region_key_seq'),
        ('gold_dim_indicator','indicator_key','gold_dim_indicator_indicator_key_seq')
    ) AS sequences(table_name,column_name,new_name)
    LOOP
        target_oid := to_regclass(format('taldau.%I',item.table_name));
        IF target_oid IS NOT NULL THEN
            sequence_name := pg_get_serial_sequence(format('taldau.%I',item.table_name),item.column_name);
        ELSE
            sequence_name := NULL;
        END IF;
        IF sequence_name IS NOT NULL THEN
            sequence_oid := to_regclass(sequence_name);
            SELECT relname INTO desired_name FROM pg_class WHERE oid=sequence_oid;
            IF desired_name<>item.new_name THEN
                IF to_regclass(format('taldau.%I',item.new_name)) IS NOT NULL THEN
                    RAISE EXCEPTION 'Target identity sequence taldau.% already exists',item.new_name;
                END IF;
                EXECUTE format('ALTER SEQUENCE %s RENAME TO %I',sequence_oid::regclass,item.new_name);
            END IF;
        END IF;
    END LOOP;

    -- Match fresh-install constraint names without dropping or rebuilding constraints.
    FOR item IN SELECT * FROM (VALUES
        ('elt_pipelines','metadata_elt_pipelines'),
        ('taldau_indicators','metadata_taldau_indicators'),
        ('extraction_runs','bronze_extraction_runs'),
        ('taldau_api_raw','bronze_taldau_api_raw'),
        ('inv_snapshots','bronze_inv_snapshots'),
        ('inv_chunks','bronze_inv_chunks'),
        ('inv_request_tasks','bronze_inv_request_tasks'),
        ('inv_snapshot_members','bronze_inv_snapshot_members'),
        ('inv_reuse_raw','bronze_inv_reuse_raw'),
        ('dim_territory','silver_dim_territory'),
        ('inv_fixed_assets','silver_inv_fixed_assets'),
        ('statistics_region','silver_statistics_region'),
        ('inv_year_cells','staging_inv_year_cells'),
        ('inv_month_expectations','quality_inv_month_expectations'),
        ('inv_snapshot_checks','quality_inv_snapshot_checks'),
        ('inv_month_diagnostics','quality_inv_month_diagnostics'),
        ('ets_inv_datasets','reconciliation_ets_inv_datasets'),
        ('ets_inv_raw','reconciliation_ets_inv_raw'),
        ('ets_inv_key_map','reconciliation_ets_inv_key_map'),
        ('inv_results','reconciliation_inv_results'),
        ('inv_differences','reconciliation_inv_differences'),
        ('dim_inv_member','gold_dim_inv_member'),
        ('dim_inv_period','gold_dim_inv_period'),
        ('fact_inv_fixed_assets','gold_fact_inv_fixed_assets'),
        ('dim_region','gold_dim_region'),
        ('dim_indicator','gold_dim_indicator'),
        ('dim_date','gold_dim_date'),
        ('fact_statistics','gold_fact_statistics')
    ) AS tables(old_name,new_name)
    LOOP
        target_oid := to_regclass(format('taldau.%I',item.new_name));
        IF target_oid IS NOT NULL THEN
            FOR dependency IN SELECT conname FROM pg_constraint
                              WHERE conrelid=target_oid
                                AND left(conname,length(item.old_name)+1)=item.old_name||'_'
            LOOP
                desired_name := item.new_name||substr(dependency.conname,length(item.old_name)+1);
                IF EXISTS(SELECT 1 FROM pg_constraint
                          WHERE conrelid=target_oid AND conname=desired_name) THEN
                    RAISE EXCEPTION 'Target constraint % already exists on taldau.%',desired_name,item.new_name;
                END IF;
                EXECUTE format('ALTER TABLE taldau.%I RENAME CONSTRAINT %I TO %I',
                    item.new_name,dependency.conname,desired_name);
            END LOOP;
        END IF;
    END LOOP;

    target_oid := to_regclass('taldau.bronze_inv_snapshots');
    IF target_oid IS NOT NULL AND EXISTS(
              SELECT 1 FROM pg_constraint WHERE conrelid=target_oid
              AND conname='inv_snapshot_range_check') THEN
        IF EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=target_oid
                  AND conname='bronze_inv_snapshot_range_check') THEN
            RAISE EXCEPTION 'Both legacy and target snapshot range constraints exist';
        END IF;
        ALTER TABLE taldau.bronze_inv_snapshots
            RENAME CONSTRAINT inv_snapshot_range_check TO bronze_inv_snapshot_range_check;
    END IF;
END
$migration$;

-- Shared legacy schemas are intentionally retained even when empty. Their ownership and use by
-- other Smart Aqmola components cannot be proven by this project migration.
