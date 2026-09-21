-- ETS is optional at deployment. No reference to its absent local table is resolved until capture.
CREATE SCHEMA IF NOT EXISTS taldau;
CREATE TABLE IF NOT EXISTS taldau.reconciliation_ets_inv_datasets (
    dataset_id text PRIMARY KEY,source_relation text NOT NULL,year integer NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now(),row_count bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS taldau.reconciliation_ets_inv_raw (
    row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES taldau.reconciliation_ets_inv_datasets,
    source_row jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS reconciliation_ets_inv_dataset_idx ON taldau.reconciliation_ets_inv_raw(dataset_id);
CREATE TABLE IF NOT EXISTS taldau.reconciliation_ets_inv_key_map (
    dataset_id text NOT NULL REFERENCES taldau.reconciliation_ets_inv_datasets,
    dimension text NOT NULL CHECK(dimension IN ('kato','krp','sif','gsvziok')),
    ets_key text NOT NULL,source_id bigint NOT NULL,
    evidence text NOT NULL CHECK(length(btrim(evidence))>0),
    PRIMARY KEY(dataset_id,dimension,ets_key)
);
COMMENT ON TABLE taldau.reconciliation_ets_inv_key_map IS
    'Reviewed ETS technical key -> Taldau source ID mapping. Never match display names automatically; even equal numeric IDs require evidence.';

CREATE OR REPLACE FUNCTION taldau.reconciliation_capture_inv_ets(p_dataset text,p_source regclass,p_year int DEFAULT 2025)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE total bigint;
BEGIN
    IF p_year<>2025 THEN RAISE EXCEPTION 'Only the 2025 migration is enabled'; END IF;
    IF EXISTS(SELECT 1 FROM taldau.reconciliation_ets_inv_datasets WHERE dataset_id=p_dataset) THEN
        RAISE EXCEPTION 'ETS dataset is immutable; use a new dataset_id for a new snapshot';
    END IF;
    INSERT INTO taldau.reconciliation_ets_inv_datasets(dataset_id,source_relation,year)
    VALUES(p_dataset,p_source::text,p_year);
    -- One statement = one MVCC source snapshot. Original space_element_set_id remains in source_row.
    EXECUTE format('INSERT INTO taldau.reconciliation_ets_inv_raw(dataset_id,source_row)
        SELECT $1,to_jsonb(e) FROM %s e WHERE e.ryear::text=$2',p_source)
    USING p_dataset,p_year::text;
    GET DIAGNOSTICS total=ROW_COUNT;
    IF total=0 THEN RAISE EXCEPTION 'ETS capture is empty; check ryear/source table'; END IF;
    UPDATE taldau.reconciliation_ets_inv_datasets SET row_count=total WHERE dataset_id=p_dataset;
    RETURN total;
END $$;

CREATE OR REPLACE VIEW taldau.reconciliation_v_ets_inv_resolved AS
SELECT e.dataset_id,e.row_id,701827::bigint AS indicator_id,
    taldau.bronze_inv_int(e.source_row->>'reporting_period') AS reporting_period,
    k.source_id AS kato_id,r.source_id AS krp_id,s.source_id AS sif_id,g.source_id AS gsvziok_id,
    CASE WHEN e.source_row->>'value' ~ '^[+-]?[0-9]+([.][0-9]+)?$'
         THEN (e.source_row->>'value')::numeric END AS value,
    e.source_row
FROM taldau.reconciliation_ets_inv_raw e
LEFT JOIN taldau.reconciliation_ets_inv_key_map k ON k.dataset_id=e.dataset_id AND k.dimension='kato' AND k.ets_key=e.source_row->>'kato1'
LEFT JOIN taldau.reconciliation_ets_inv_key_map r ON r.dataset_id=e.dataset_id AND r.dimension='krp' AND r.ets_key=e.source_row->>'krp'
LEFT JOIN taldau.reconciliation_ets_inv_key_map s ON s.dataset_id=e.dataset_id AND s.dimension='sif' AND s.ets_key=e.source_row->>'sif'
LEFT JOIN taldau.reconciliation_ets_inv_key_map g ON g.dataset_id=e.dataset_id AND g.dimension='gsvziok' AND g.ets_key=e.source_row->>'gsvziok';

CREATE TABLE IF NOT EXISTS taldau.reconciliation_inv_results (
    snapshot_id text NOT NULL,dataset_id text NOT NULL,reporting_period bigint NOT NULL,
    missing_in_taldau bigint NOT NULL,extra_in_taldau bigint NOT NULL,value_mismatch bigint NOT NULL,
    matched bigint NOT NULL,duplicates_taldau bigint NOT NULL,duplicates_ets bigint NOT NULL,
    ambiguous_combinations bigint NOT NULL,compared_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(snapshot_id,dataset_id,reporting_period)
);
CREATE TABLE IF NOT EXISTS taldau.reconciliation_inv_differences (
    snapshot_id text NOT NULL,dataset_id text NOT NULL,reporting_period bigint NOT NULL,
    kato_id bigint NOT NULL,krp_id bigint NOT NULL,sif_id bigint NOT NULL,gsvziok_id bigint NOT NULL,
    difference text NOT NULL,taldau_value numeric,ets_value numeric,
    PRIMARY KEY(snapshot_id,dataset_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id)
);
CREATE OR REPLACE FUNCTION taldau.reconciliation_compare_inv_ets(p_snapshot text,p_dataset text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE total bigint;
BEGIN
    IF NOT EXISTS(SELECT 1 FROM taldau.bronze_inv_snapshots WHERE snapshot_id=p_snapshot AND state='published') THEN
        RAISE EXCEPTION 'Publish a complete validated snapshot before comparing Silver';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM taldau.reconciliation_ets_inv_datasets WHERE dataset_id=p_dataset AND row_count>0) THEN
        RAISE EXCEPTION 'Capture an ETS dataset first';
    END IF;
    IF EXISTS(SELECT 1 FROM taldau.reconciliation_v_ets_inv_resolved WHERE dataset_id=p_dataset AND
        (kato_id IS NULL OR krp_id IS NULL OR sif_id IS NULL OR gsvziok_id IS NULL OR reporting_period IS NULL OR value IS NULL)) THEN
        RAISE EXCEPTION 'ETS has unmapped technical keys or invalid values; inspect v_ets_inv_resolved. No name-based fallback.';
    END IF;
    -- Do not compare against a snapshot that has since been replaced or partly overwritten.
    IF EXISTS (
      (SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value FROM taldau.staging_inv_year_cells
       WHERE snapshot_id=p_snapshot AND value IS NOT NULL
       EXCEPT SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value FROM taldau.silver_inv_fixed_assets
       WHERE source_snapshot_id=p_snapshot)
      UNION ALL
      (SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value FROM taldau.silver_inv_fixed_assets
       WHERE source_snapshot_id=p_snapshot
       EXCEPT SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value FROM taldau.staging_inv_year_cells
       WHERE snapshot_id=p_snapshot AND value IS NOT NULL)
    ) THEN RAISE EXCEPTION 'Silver no longer matches this published snapshot'; END IF;

    -- Pre-count duplicate keys; never sum them or allow many-to-many joins to inflate matches.
    CREATE TEMP TABLE IF NOT EXISTS inv_comparison_work (
        indicator_id bigint,reporting_period bigint,kato_id bigint,krp_id bigint,sif_id bigint,gsvziok_id bigint,
        direct_n bigint,ets_n bigint,taldau_value numeric,ets_value numeric,difference text
    ) ON COMMIT DROP;
    TRUNCATE TABLE pg_temp.inv_comparison_work;
    INSERT INTO pg_temp.inv_comparison_work
    WITH d AS (
        SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,count(*) AS n,min(value) AS value
        FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=p_snapshot GROUP BY 1,2,3,4,5,6
    ), e AS (
        SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,count(*) AS n,min(value) AS value
        FROM taldau.reconciliation_v_ets_inv_resolved WHERE dataset_id=p_dataset GROUP BY 1,2,3,4,5,6
    )
    SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,
        coalesce(d.n,0) AS direct_n,coalesce(e.n,0) AS ets_n,
        CASE WHEN d.n=1 THEN d.value END AS taldau_value,
        CASE WHEN e.n=1 THEN e.value END AS ets_value,
        CASE WHEN d.n>1 OR e.n>1 THEN 'duplicate_key'
             WHEN d.n IS NULL THEN 'missing_in_taldau'
             WHEN e.n IS NULL THEN 'extra_in_taldau'
             WHEN d.value IS DISTINCT FROM e.value THEN 'value_mismatch'
             ELSE 'matched' END AS difference
    FROM d FULL OUTER JOIN e USING(indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id);
    DELETE FROM taldau.reconciliation_inv_results WHERE snapshot_id=p_snapshot AND dataset_id=p_dataset;
    DELETE FROM taldau.reconciliation_inv_differences WHERE snapshot_id=p_snapshot AND dataset_id=p_dataset;
    INSERT INTO taldau.reconciliation_inv_results
    SELECT p_snapshot,p_dataset,reporting_period,
        count(*) FILTER(WHERE difference='missing_in_taldau'),count(*) FILTER(WHERE difference='extra_in_taldau'),
        count(*) FILTER(WHERE difference='value_mismatch'),count(*) FILTER(WHERE difference='matched'),
        coalesce(sum(greatest(direct_n-1,0)),0),coalesce(sum(greatest(ets_n-1,0)),0),
        count(*) FILTER(WHERE difference='duplicate_key'),now()
    FROM pg_temp.inv_comparison_work GROUP BY reporting_period;
    INSERT INTO taldau.reconciliation_inv_differences
    SELECT p_snapshot,p_dataset,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,difference,taldau_value,ets_value
    FROM pg_temp.inv_comparison_work WHERE difference<>'matched';
    GET DIAGNOSTICS total=ROW_COUNT;
    RETURN total;
END $$;
