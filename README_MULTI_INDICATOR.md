# Multi-indicator Taldau ELT

`taldau_statistics_2023_2026` is the paused production entry point for Apache Airflow 2.9.2.
One manual batch freezes one independent snapshot per enabled indicator, then runs bounded discovery and
chunk waves through the shared `taldau_api` pool. The DAG ends after per-indicator validation,
diagnostics and a batch summary. It never publishes automatically.

`taldau_statistics_incremental` is the separate, initially paused monthly entry point. It refreshes
only the logical run year by default and supports explicit backfills. See
[the incremental operations guide](README_INCREMENTAL.md) for parameters, first launch, recovery,
publication checks and rollout. Both entry points use `taldau_elt/orchestration.py`.

## Registry

`taldau.metadata_indicator_registry` is authoritative. `taldau.metadata_enabled_indicators` exposes only
enabled rows with a usable source configuration. Migration `011_indicator_registry_sources.sql` enables
the eight verified sources below without changing source configs already frozen in Bronze snapshots:

| indicator key | source ID | period ID | frequency | period semantics | idx |
|---|---:|---:|---|---|---:|
| `investments_fixed_assets` | 701827 | 8 | monthly | cumulative | 3 |
| `population` | 703831 | 7 | annual | point_in_time_start_period | 3 |
| `average_salary` | 702972 | 5 | quarterly | period | 0 |
| `grp` | 2709379 | 9 | quarterly | cumulative | 0 |
| `agriculture` | 701189 | 8 | monthly | cumulative | 0 |
| `industry` | 701592 | 8 | monthly | cumulative | 1 |
| `trade` | 2709782 | 4 | monthly | period | 0 |
| `construction` | 701885 | 8 | monthly | cumulative | 0 |

Real Taldau codes use `MMYYYY`: monthly accepts every month, quarterly accepts quarter-end months
`03/06/09/12`, and annual accepts `12YYYY`. Legacy `QnYYYY` and `YYYY` parsing remains supported.
Completed historical years are expected to contain 12, 4 or 1 periods respectively. Missing periods in
the current year remain a non-blocking diagnostic.

## Generic objects and compatibility

Migrations `010_multi_indicator_framework.sql` and `011_indicator_registry_sources.sql` are additive and
idempotent. They do not drop schemas or legacy objects. The authoritative mapping is:

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

The old `inv_*` tables, functions and reports remain available as deprecated compatibility paths.
The three legacy DAG files (`taldau_pipeline`, `taldau_inv_fixed_assets`,
`taldau_inv_fixed_assets_2025`) have been removed from active discovery; their source remains in Git
history. `public.bns_*` is external and migration 010 never changes it.

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

Migration `012_incremental_refresh.sql` extends the eight enabled registry horizons to the existing
generic SQL domain limit (2100), adds durable incremental batch ownership and source-period cutoff
filtering, and declares `gold_fact_observations_lookup_idx`. Frozen historical snapshots and the
published `taldau-statistics-2023-2026-prod-v2` batch are not modified. Fresh bootstrap uses the same
registry horizon and index. There is no annual code or migration change for 2027, 2028, etc.
