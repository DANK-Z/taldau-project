# Multi-indicator Taldau ELT

`taldau_statistics_2023_2026` is the paused production entry point for Apache Airflow 2.9.2.
One manual batch freezes one independent snapshot per enabled indicator, then runs bounded discovery and
chunk waves through the shared `taldau_api` pool. The DAG ends after per-indicator validation,
diagnostics and a batch summary. It never publishes automatically.

## Registry

`taldau.metadata_indicator_registry` is authoritative. `taldau.metadata_enabled_indicators` exposes only
enabled rows with a usable source configuration. Investments are enabled with the verified source values:

| field | value |
|---|---|
| `indicator_key` | `investments_fixed_assets` |
| `indicator_id` | `701827` |
| `period_id` | `8` |
| dimensions | `kato=68`, `krp=90`, `sif=459`, `gsvziok=4043` |
| roots | `741880`, `741927`, `807855`, `19202525` |
| frequency | monthly |

Population, average salary, GRP, agriculture, industry, trade and construction are catalog rows with
`enabled=false` and NULL source IDs/config. The DAG cannot schedule them. To add an indicator, insert or
update one registry row with verified IDs, ordered dimensions, a root for every dimension and extraction
metadata, then enable it. The Python DAG and traversal code do not change.

## Generic objects and compatibility

Migration `010_multi_indicator_framework.sql` is additive and idempotent. It does not drop schemas or
legacy objects. The authoritative mapping is:

| investments-only | generic authoritative object |
|---|---|
| `bronze_inv_snapshots` | `bronze_snapshots` |
| `bronze_inv_chunks` | `bronze_chunks` |
| `bronze_inv_request_tasks` | `bronze_request_tasks` |
| `bronze_inv_snapshot_members` | `bronze_snapshot_members` |
| `bronze_inv_reuse_raw` | `bronze_reuse_raw` |
| `bronze_inv_run_raw` | `bronze_run_raw` |
| `staging_inv_year_cells` | `staging_observation_cells` |
| `quality_inv_snapshot_checks` | `quality_snapshot_checks` |
| `quality_inv_month_diagnostics` | `quality_period_diagnostics` |
| `silver_inv_fixed_assets` | `silver_observations` |
| `gold_dim_inv_member` | `gold_dim_member` |
| `gold_dim_inv_period` | `gold_dim_period` |
| `gold_fact_inv_fixed_assets` | `gold_fact_observations` |

`bronze_taldau_api_raw` remains the shared lossless transport table. JSON bodies and `x` are unchanged.
`staging_observation_cells.coordinates` stores arbitrary source member IDs as JSONB, while
`coordinate_hash` supplies a deterministic indexed key. The unique constraints also compare the full
JSONB value, so a hash is not treated as proof of equality. NUMERIC values remain exact.

The old `inv_*` tables, functions, reports and three DAG IDs remain available as deprecated compatibility
paths. `public.bns_*` is external and migration 010 never changes it.

## Snapshots, resume and migration

Each indicator has its own frozen `bronze_snapshots` row. `bronze_batches` groups those snapshots only for
orchestration and reporting. Failed chunks and snapshots are isolated; a later manual trigger with
`resume_failed=true` requeues only failed work. Completed chunks are never remapped. Each wave contains at
most 128 scalar chunk IDs, and raw payloads never enter XCom.

Migration 010 copies `kz-investments-2023-2026-prod-v1` into the generic model with the same snapshot ID,
state, years, frozen config and discovery run. It is attached to batch
`taldau-statistics-2023-2026-prod-v1`. No extraction or publication is performed by the migration.

## Commands

These commands use local `PG*` settings. Applying them to production remains a separate reviewed action.

```powershell
.venv/Scripts/python.exe tools/manage_statistics_batch.py migrate
.venv/Scripts/python.exe tools/manage_statistics_batch.py prepare --batch-id taldau-statistics-2023-2026-prod-v1
.venv/Scripts/python.exe tools/manage_statistics_batch.py status --batch-id taldau-statistics-2023-2026-prod-v1
```

The launch command checks that the global Airflow pool exists with exactly three slots:

```powershell
.venv/Scripts/python.exe tools/manage_statistics_batch.py launch `
  --batch-id taldau-statistics-2023-2026-prod-v1 --confirm-extraction
```

After validation and review, publication is explicit and snapshot-scoped:

```powershell
.venv/Scripts/python.exe tools/manage_statistics_batch.py snapshot `
  --snapshot-id kz-investments-2023-2026-prod-v1
.venv/Scripts/python.exe tools/manage_statistics_batch.py publish `
  --snapshot-id kz-investments-2023-2026-prod-v1
```

`taldau.publish_snapshot` validates again and replaces only the indicator/year scope inside one database
transaction. Any Silver/Gold count or value mismatch raises and rolls back both layers.
