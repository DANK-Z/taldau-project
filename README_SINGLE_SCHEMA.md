# Single-schema layout для production

Fresh install migrations 001–008 создаёт только schema `taldau`. Слои сохранены в именах
объектов. Migration `009_single_taldau_schema.sql` предназначена для production-инсталляций,
где legacy migrations 001–008 уже были применены. Она переносит только перечисленные ниже
Taldau-объекты через `ALTER ... SET SCHEMA` и `RENAME`, сохраняя данные, OID, constraints,
indexes, foreign keys и зависимости.

## Mapping relations

| Legacy | Single schema |
|---|---|
| `metadata.elt_pipelines` | `taldau.metadata_elt_pipelines` |
| `metadata.taldau_indicators` | `taldau.metadata_taldau_indicators` |
| `bronze.extraction_runs` | `taldau.bronze_extraction_runs` |
| `bronze.taldau_api_raw` | `taldau.bronze_taldau_api_raw` |
| `bronze.v_taldau_tree_nodes` | `taldau.bronze_v_taldau_tree_nodes` |
| `bronze.v_inv_candidates` | `taldau.bronze_v_inv_candidates` |
| `bronze.inv_snapshots` | `taldau.bronze_inv_snapshots` |
| `bronze.inv_chunks` | `taldau.bronze_inv_chunks` |
| `bronze.inv_request_tasks` | `taldau.bronze_inv_request_tasks` |
| `bronze.inv_snapshot_members` | `taldau.bronze_inv_snapshot_members` |
| `bronze.inv_reuse_raw` | `taldau.bronze_inv_reuse_raw` |
| `bronze.inv_run_raw` | `taldau.bronze_inv_run_raw` |
| `silver.dim_territory` | `taldau.silver_dim_territory` |
| `silver.inv_fixed_assets` | `taldau.silver_inv_fixed_assets` |
| `silver.statistics_region` | `taldau.silver_statistics_region` |
| `staging.inv_year_cells` | `taldau.staging_inv_year_cells` |
| `quality.inv_month_expectations` | `taldau.quality_inv_month_expectations` |
| `quality.inv_snapshot_checks` | `taldau.quality_inv_snapshot_checks` |
| `quality.inv_month_diagnostics` | `taldau.quality_inv_month_diagnostics` |
| `quality.inv_year_coverage` | `taldau.quality_inv_year_coverage` |
| `reconciliation.ets_inv_datasets` | `taldau.reconciliation_ets_inv_datasets` |
| `reconciliation.ets_inv_raw` | `taldau.reconciliation_ets_inv_raw` |
| `reconciliation.ets_inv_key_map` | `taldau.reconciliation_ets_inv_key_map` |
| `reconciliation.v_ets_inv_resolved` | `taldau.reconciliation_v_ets_inv_resolved` |
| `reconciliation.inv_results` | `taldau.reconciliation_inv_results` |
| `reconciliation.inv_differences` | `taldau.reconciliation_inv_differences` |
| `gold.dim_inv_member` | `taldau.gold_dim_inv_member` |
| `gold.dim_inv_period` | `taldau.gold_dim_inv_period` |
| `gold.fact_inv_fixed_assets` | `taldau.gold_fact_inv_fixed_assets` |
| `gold.v_inv_fixed_assets` | `taldau.gold_v_inv_fixed_assets` |
| `gold.mart_inv_territory_totals` | `taldau.gold_mart_inv_territory_totals` |
| `gold.dim_region` | `taldau.gold_dim_region` |
| `gold.dim_indicator` | `taldau.gold_dim_indicator` |
| `gold.dim_date` | `taldau.gold_dim_date` |
| `gold.fact_statistics` | `taldau.gold_fact_statistics` |
| `gold.v_region_year_metrics` | `taldau.gold_v_region_year_metrics` |

## Mapping functions

| Legacy | Single schema |
|---|---|
| `bronze.inv_int(text)` | `taldau.bronze_inv_int(text)` |
| `bronze.stage_inv_run(text,text)` | `taldau.bronze_stage_inv_run(text,text)` |
| `bronze.validate_inv_pilot(text)` | `taldau.bronze_validate_inv_pilot(text)` |
| `staging.stage_inv_chunk(bigint)` | `taldau.staging_stage_inv_chunk(bigint)` |
| `quality.inv_run_coverage(text)` | `taldau.quality_inv_run_coverage(text)` |
| `quality.inv_discovery_errors(text)` | `taldau.quality_inv_discovery_errors(text)` |
| `quality.validate_inv_snapshot(text)` | `taldau.quality_validate_inv_snapshot(text)` |
| `silver.refresh_inv_pilot(text)` | `taldau.silver_refresh_inv_pilot(text)` |
| `silver.publish_inv_snapshot(text)` | `taldau.silver_publish_inv_snapshot(text)` |
| `gold.refresh_inv_pilot(text)` | `taldau.gold_refresh_inv_pilot(text)` |
| `gold.publish_inv_snapshot(text)` | `taldau.gold_publish_inv_snapshot(text)` |
| `reconciliation.capture_inv_ets(text,regclass,integer)` | `taldau.reconciliation_capture_inv_ets(text,regclass,integer)` |
| `reconciliation.compare_inv_ets(text,text)` | `taldau.reconciliation_compare_inv_ets(text,text)` |

## Mapping indexes и identity sequences

| Legacy | Single schema |
|---|---|
| `bronze.taldau_raw_run_dimension_idx` | `taldau.bronze_taldau_raw_run_dimension_idx` |
| `silver.inv_fixed_assets_run_idx` | `taldau.silver_inv_fixed_assets_run_idx` |
| `bronze.inv_chunks_pending_idx` | `taldau.bronze_inv_chunks_pending_idx` |
| `bronze.inv_members_lookup_idx` | `taldau.bronze_inv_members_lookup_idx` |
| `bronze.inv_members_run_idx` | `taldau.bronze_inv_members_run_idx` |
| `staging.inv_cells_chunk_idx` | `taldau.staging_inv_cells_chunk_idx` |
| `staging.inv_cells_grain_idx` | `taldau.staging_inv_cells_grain_idx` |
| `silver.inv_silver_snapshot_idx` | `taldau.silver_inv_snapshot_idx` |
| `reconciliation.ets_inv_dataset_idx` | `taldau.reconciliation_ets_inv_dataset_idx` |
| `bronze.inv_reuse_raw_id_idx` | `taldau.bronze_inv_reuse_raw_id_idx` |
| `bronze.taldau_api_raw_id_seq` | `taldau.bronze_taldau_api_raw_id_seq` |
| `bronze.inv_chunks_chunk_id_seq` | `taldau.bronze_inv_chunks_chunk_id_seq` |
| `reconciliation.ets_inv_raw_row_id_seq` | `taldau.reconciliation_ets_inv_raw_row_id_seq` |
| `gold.dim_inv_member_member_key_seq` | `taldau.gold_dim_inv_member_member_key_seq` |
| `gold.dim_region_region_key_seq` | `taldau.gold_dim_region_region_key_seq` |
| `gold.dim_indicator_indicator_key_seq` | `taldau.gold_dim_indicator_indicator_key_seq` |

Generated constraint names with an old table prefix are renamed to the new table prefix without
dropping or rebuilding the constraint. The explicit `inv_snapshot_range_check` becomes
`bronze_inv_snapshot_range_check`.

## Безопасность production migration

`009_single_taldau_schema.sql` не содержит `DROP SCHEMA` и не удаляет relations. Если legacy и
target object существуют одновременно, migration завершается ошибкой вместо автоматического
merge. Schemas `metadata`, `bronze`, `silver`, `gold`, `staging`, `quality`, `reconciliation`
и `public` всегда остаются на месте, включая любые посторонние объекты Smart Aqmola.

На существующей legacy installation команда `manage_investments_snapshot.py migrate` выполняет
009 первой, затем идемпотентно переигрывает актуальные 004–008. На fresh install сначала
применяются 001–003, после чего эта же команда создаёт snapshot objects. Все файлы выполняются
одной application transaction; production migration требует обычного backup/change window.

Внешняя ETS-таблица `public.bns_inv_fixed_assets` не принадлежит Taldau ELT и остаётся без изменений.
Airflow использует существующий Connection ID `digest_target_db`.
