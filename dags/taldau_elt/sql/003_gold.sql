CREATE SCHEMA IF NOT EXISTS gold;
CREATE TABLE IF NOT EXISTS gold.dim_inv_member (
    member_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    indicator_id bigint NOT NULL,
    dimension text NOT NULL,
    source_id bigint NOT NULL,
    member_name text NOT NULL,
    parent_source_id bigint,
    source_tree_depth integer NOT NULL,
    is_total boolean NOT NULL,
    source_run_id text NOT NULL REFERENCES bronze.extraction_runs,
    UNIQUE(indicator_id,dimension,source_id)
);
CREATE TABLE IF NOT EXISTS gold.dim_inv_period (
    indicator_id bigint NOT NULL,
    reporting_period bigint NOT NULL,
    period_code text NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    period_type text NOT NULL CHECK(period_type='month_cumulative'),
    PRIMARY KEY(indicator_id,reporting_period)
);
CREATE TABLE IF NOT EXISTS gold.fact_inv_fixed_assets (
    indicator_id bigint NOT NULL,
    reporting_period bigint NOT NULL,
    kato_key bigint NOT NULL REFERENCES gold.dim_inv_member,
    krp_key bigint NOT NULL REFERENCES gold.dim_inv_member,
    sif_key bigint NOT NULL REFERENCES gold.dim_inv_member,
    gsvziok_key bigint NOT NULL REFERENCES gold.dim_inv_member,
    value numeric NOT NULL,
    value_measure text,
    source_run_id text NOT NULL REFERENCES bronze.extraction_runs,
    source_raw_id bigint NOT NULL REFERENCES bronze.taldau_api_raw,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(indicator_id,reporting_period,kato_key,krp_key,sif_key,gsvziok_key),
    FOREIGN KEY(indicator_id,reporting_period) REFERENCES gold.dim_inv_period
);
CREATE OR REPLACE VIEW gold.v_inv_fixed_assets AS
SELECT f.indicator_id,f.reporting_period,p.period_code,p.start_date,p.end_date,p.period_type,
    k.source_id AS kato_id,k.member_name AS kato_name,k.parent_source_id AS kato_parent_id,
    k.source_tree_depth AS kato_tree_depth,
    r.source_id AS krp_id,r.member_name AS krp_name,r.is_total AS krp_is_total,
    s.source_id AS sif_id,s.member_name AS sif_name,s.is_total AS sif_is_total,
    g.source_id AS gsvziok_id,g.member_name AS gsvziok_name,g.is_total AS gsvziok_is_total,
    r.parent_source_id AS krp_parent_id,s.parent_source_id AS sif_parent_id,
    g.parent_source_id AS gsvziok_parent_id,
    f.value,f.value_measure,f.source_run_id,f.source_raw_id
FROM gold.fact_inv_fixed_assets f
JOIN gold.dim_inv_period p USING(indicator_id,reporting_period)
JOIN gold.dim_inv_member k ON k.member_key=f.kato_key AND k.dimension='kato'
JOIN gold.dim_inv_member r ON r.member_key=f.krp_key AND r.dimension='krp'
JOIN gold.dim_inv_member s ON s.member_key=f.sif_key AND s.dimension='sif'
JOIN gold.dim_inv_member g ON g.member_key=f.gsvziok_key AND g.dimension='gsvziok';

-- Source-provided totals only: no sum over parents/children or cumulative months.
CREATE OR REPLACE VIEW gold.mart_inv_territory_totals AS
SELECT indicator_id,kato_id,kato_name,kato_parent_id,kato_tree_depth,
       reporting_period,period_code,start_date,end_date,value,value_measure,source_run_id
FROM gold.v_inv_fixed_assets WHERE krp_is_total AND sif_is_total AND gsvziok_is_total;

CREATE OR REPLACE FUNCTION gold.refresh_inv_pilot(p_run text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE r bronze.extraction_runs; inserted bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('silver.inv_fixed_assets:pilot',0));
    SELECT * INTO STRICT r FROM bronze.extraction_runs WHERE run_id=p_run;
    IF r.status NOT IN ('silver_validated','gold_validated') THEN
        RAISE EXCEPTION 'Silver must be validated before Gold';
    END IF;
    IF (SELECT count(*) FROM silver.inv_fixed_assets WHERE source_run_id=p_run)<>(r.scope->>'expected_numeric')::bigint THEN
        RAISE EXCEPTION 'Silver slice has changed or been superseded';
    END IF;
    INSERT INTO gold.dim_inv_member
        (indicator_id,dimension,source_id,member_name,parent_source_id,source_tree_depth,is_total,source_run_id)
    SELECT DISTINCT ON(indicator_id,dimension,member_id)
        indicator_id,dimension,member_id,member_name,parent_id,tree_depth,
        member_id::text=(r.config->'roots'->>dimension),run_id
    FROM bronze.v_taldau_tree_nodes WHERE run_id=p_run
    ORDER BY indicator_id,dimension,member_id,raw_id
    ON CONFLICT(indicator_id,dimension,source_id) DO UPDATE SET
        member_name=EXCLUDED.member_name,parent_source_id=EXCLUDED.parent_source_id,
        source_tree_depth=EXCLUDED.source_tree_depth,is_total=EXCLUDED.is_total,
        source_run_id=EXCLUDED.source_run_id;
    INSERT INTO gold.dim_inv_period
    SELECT DISTINCT indicator_id,reporting_period,period_code,start_date,end_date,'month_cumulative'
    FROM silver.inv_fixed_assets WHERE source_run_id=p_run
    ON CONFLICT(indicator_id,reporting_period) DO UPDATE SET
        period_code=EXCLUDED.period_code,start_date=EXCLUDED.start_date,end_date=EXCLUDED.end_date;
    DELETE FROM gold.fact_inv_fixed_assets f USING gold.dim_inv_member k
    WHERE k.member_key=f.kato_key AND k.source_id=(r.scope->>'kato_id')::bigint
        AND f.indicator_id=(r.config->>'indicator_id')::bigint
        AND f.reporting_period=(r.scope->>'reporting_period')::bigint;
    INSERT INTO gold.fact_inv_fixed_assets
    (indicator_id,reporting_period,kato_key,krp_key,sif_key,gsvziok_key,value,value_measure,source_run_id,source_raw_id)
    SELECT s.indicator_id,s.reporting_period,k.member_key,rp.member_key,si.member_key,g.member_key,
           s.value,s.value_measure,s.source_run_id,s.source_raw_id
    FROM silver.inv_fixed_assets s
    JOIN gold.dim_inv_member k ON k.indicator_id=s.indicator_id AND k.dimension='kato' AND k.source_id=s.kato_id
    JOIN gold.dim_inv_member rp ON rp.indicator_id=s.indicator_id AND rp.dimension='krp' AND rp.source_id=s.krp_id
    JOIN gold.dim_inv_member si ON si.indicator_id=s.indicator_id AND si.dimension='sif' AND si.source_id=s.sif_id
    JOIN gold.dim_inv_member g ON g.indicator_id=s.indicator_id AND g.dimension='gsvziok' AND g.source_id=s.gsvziok_id
    WHERE s.source_run_id=p_run;
    GET DIAGNOSTICS inserted = ROW_COUNT;
    IF inserted<>(r.scope->>'expected_numeric')::bigint THEN RAISE EXCEPTION 'Gold row loss'; END IF;
    -- Validate keys and values in both directions before committing publication.
    IF EXISTS (
        (SELECT kato_id,krp_id,sif_id,gsvziok_id,reporting_period,value FROM silver.inv_fixed_assets WHERE source_run_id=p_run
         EXCEPT SELECT kato_id,krp_id,sif_id,gsvziok_id,reporting_period,value FROM gold.v_inv_fixed_assets WHERE source_run_id=p_run)
        UNION ALL
        (SELECT kato_id,krp_id,sif_id,gsvziok_id,reporting_period,value FROM gold.v_inv_fixed_assets WHERE source_run_id=p_run
         EXCEPT SELECT kato_id,krp_id,sif_id,gsvziok_id,reporting_period,value FROM silver.inv_fixed_assets WHERE source_run_id=p_run)
    ) THEN RAISE EXCEPTION 'Gold/Silver mismatch'; END IF;
    UPDATE bronze.extraction_runs SET status='gold_validated' WHERE run_id=p_run;
    RETURN inserted;
END $$;
