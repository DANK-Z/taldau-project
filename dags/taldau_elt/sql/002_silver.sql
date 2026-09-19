CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW bronze.v_taldau_tree_nodes AS
SELECT r.id AS raw_id,r.run_id,r.indicator_id,r.dimension,r.tree_depth,
       r.request_params,r.loaded_at,n.ordinality AS node_ordinal,n.node,
       (n.node->>'id')::bigint AS member_id,
       btrim(n.node->>'text') AS member_name,
       NULLIF(r.request_params->>'p_parent_id','')::bigint AS parent_id
FROM bronze.taldau_api_raw r
CROSS JOIN LATERAL jsonb_array_elements(r.response_data) WITH ORDINALITY AS n(node,ordinality);

CREATE OR REPLACE VIEW bronze.v_inv_candidates AS
SELECT n.run_id,n.raw_id,n.node_ordinal,n.indicator_id,
       split_part(n.request_params->>'p_terms',',',1)::bigint AS kato_id,
       split_part(n.request_params->>'p_terms',',',2)::bigint AS krp_id,
       split_part(n.request_params->>'p_terms',',',3)::bigint AS sif_id,
       n.member_id AS gsvziok_id,n.member_name AS gsvziok_name,
       r.scope->>'period_code' AS period_code,
       n.node->>(r.scope->>'period_code') AS reporting_period_text,
       n.node->>('y'||(r.scope->>'period_code')) AS raw_value,
       n.node->>'measureName' AS value_measure,
       CASE WHEN n.node->>('y'||(r.scope->>'period_code')) ~ '^[+-]?[0-9]+([.][0-9]+)?$'
            THEN (n.node->>('y'||(r.scope->>'period_code')))::numeric END AS value
FROM bronze.v_taldau_tree_nodes n
JOIN bronze.extraction_runs r USING(run_id)
WHERE n.dimension='gsvziok' AND n.node ? (r.scope->>'period_code');

CREATE TABLE IF NOT EXISTS silver.dim_territory (
    territory_id bigint PRIMARY KEY,
    territory_name text NOT NULL,
    territory_level text, -- Unknown until the source supplies a reliable classification.
    parent_id bigint,
    source_tree_depth integer NOT NULL,
    source_raw_id bigint NOT NULL REFERENCES bronze.taldau_api_raw,
    source_run_id text NOT NULL REFERENCES bronze.extraction_runs,
    loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS silver.inv_fixed_assets (
    indicator_id bigint NOT NULL,
    kato_id bigint NOT NULL REFERENCES silver.dim_territory,
    kato_name text NOT NULL,
    krp_id bigint NOT NULL,krp_name text NOT NULL,
    sif_id bigint NOT NULL,sif_name text NOT NULL,
    gsvziok_id bigint NOT NULL,gsvziok_name text NOT NULL,
    period_code text NOT NULL,
    reporting_period bigint NOT NULL,
    period_date date NOT NULL,start_date date NOT NULL,end_date date NOT NULL,
    value numeric NOT NULL,
    value_measure text,
    source_run_id text NOT NULL REFERENCES bronze.extraction_runs,
    source_raw_id bigint NOT NULL REFERENCES bronze.taldau_api_raw,
    source_node_ordinal bigint NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id)
);
CREATE INDEX IF NOT EXISTS inv_fixed_assets_run_idx ON silver.inv_fixed_assets(source_run_id);

CREATE OR REPLACE FUNCTION bronze.validate_inv_pilot(p_run text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE r bronze.extraction_runs; total_rows bigint; numeric_rows bigint; x_rows bigint;
        invalid_rows bigint; duplicates bigint; min_value numeric; max_value numeric;
BEGIN
    SELECT * INTO STRICT r FROM bronze.extraction_runs WHERE run_id=p_run;
    IF r.status NOT IN ('bronze_complete','silver_validated','gold_validated') THEN
        RAISE EXCEPTION 'Extraction is not complete: %',r.status;
    END IF;
    IF r.config->>'indicator_id'<>'701827' OR r.config->>'period_id'<>'8'
       OR r.scope->>'kato_id'<>'268012' OR r.scope->>'period_code'<>'122025'
       OR r.scope->>'reporting_period'<>'1069' THEN
        RAISE EXCEPTION 'Only the approved pilot scope is supported';
    END IF;
    SELECT count(*),count(value),count(*) FILTER(WHERE raw_value='x'),
           count(*) FILTER(WHERE (value IS NULL AND raw_value IS DISTINCT FROM 'x')
                OR reporting_period_text IS DISTINCT FROM (r.scope->>'reporting_period')
                OR kato_id::text IS DISTINCT FROM (r.scope->>'kato_id')),
           min(value),max(value)
    INTO total_rows,numeric_rows,x_rows,invalid_rows,min_value,max_value
    FROM bronze.v_inv_candidates WHERE run_id=p_run;
    SELECT count(*) INTO duplicates FROM (
        SELECT kato_id,krp_id,sif_id,gsvziok_id,reporting_period_text
        FROM bronze.v_inv_candidates WHERE run_id=p_run
        GROUP BY 1,2,3,4,5 HAVING count(*)>1
    ) d;
    IF invalid_rows>0 OR duplicates>0 THEN
        RAISE EXCEPTION 'Invalid rows: %, duplicate natural keys: %',invalid_rows,duplicates;
    END IF;
    -- Conflicting names/parents must not be hidden by a DISTINCT or last-write-wins.
    IF EXISTS (
        SELECT dimension,member_id FROM bronze.v_taldau_tree_nodes WHERE run_id=p_run
        GROUP BY 1,2 HAVING count(DISTINCT (member_name,parent_id,tree_depth))>1
    ) THEN RAISE EXCEPTION 'Conflicting source dimension definitions'; END IF;
    IF EXISTS (
        SELECT 1 FROM bronze.v_taldau_tree_nodes
        WHERE run_id=p_run AND (member_name IS NULL OR member_name='')
    ) THEN RAISE EXCEPTION 'Missing dimension name'; END IF;
    IF numeric_rows<>(r.scope->>'expected_numeric')::bigint OR x_rows<>(r.scope->>'expected_x')::bigint THEN
        RAISE EXCEPTION 'Pilot mismatch: numeric %, x %, total %. Inspect source; do not patch counts.',numeric_rows,x_rows,total_rows;
    END IF;
    RETURN jsonb_build_object('run_id',p_run,'combinations',total_rows,'numeric',numeric_rows,
        'x',x_rows,'duplicates',duplicates,'invalid',invalid_rows,'min_value',min_value,'max_value',max_value);
END $$;

CREATE OR REPLACE FUNCTION silver.refresh_inv_pilot(p_run text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE r bronze.extraction_runs; result jsonb;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('silver.inv_fixed_assets:pilot',0));
    SELECT * INTO STRICT r FROM bronze.extraction_runs WHERE run_id=p_run;
    result := bronze.validate_inv_pilot(p_run);
    -- An old replay must not silently overwrite a more recently acquired version.
    IF EXISTS (SELECT 1 FROM silver.inv_fixed_assets s JOIN bronze.extraction_runs other
               ON other.run_id=s.source_run_id WHERE s.kato_id=(r.scope->>'kato_id')::bigint
               AND s.reporting_period=(r.scope->>'reporting_period')::bigint
               AND other.started_at>r.started_at) THEN
        RAISE EXCEPTION 'A newer run already owns this Silver slice';
    END IF;
    INSERT INTO silver.dim_territory
        (territory_id,territory_name,parent_id,source_tree_depth,source_raw_id,source_run_id)
    SELECT DISTINCT ON(member_id) member_id,member_name,parent_id,tree_depth,raw_id,run_id
    FROM bronze.v_taldau_tree_nodes WHERE run_id=p_run AND dimension='kato'
    ORDER BY member_id,raw_id
    ON CONFLICT(territory_id) DO UPDATE SET territory_name=EXCLUDED.territory_name,
        parent_id=EXCLUDED.parent_id,source_tree_depth=EXCLUDED.source_tree_depth,
        source_raw_id=EXCLUDED.source_raw_id,source_run_id=EXCLUDED.source_run_id,loaded_at=now();

    -- Replace only this validated slice, atomically. This also removes revised numeric -> x values.
    DELETE FROM silver.inv_fixed_assets WHERE indicator_id=(r.config->>'indicator_id')::bigint
        AND kato_id=(r.scope->>'kato_id')::bigint AND reporting_period=(r.scope->>'reporting_period')::bigint;
    INSERT INTO silver.inv_fixed_assets
        (indicator_id,kato_id,kato_name,krp_id,krp_name,sif_id,sif_name,gsvziok_id,gsvziok_name,
         period_code,reporting_period,period_date,start_date,end_date,value,value_measure,
         source_run_id,source_raw_id,source_node_ordinal)
    SELECT c.indicator_id,c.kato_id,t.territory_name,c.krp_id,k.member_name,
        c.sif_id,s.member_name,c.gsvziok_id,c.gsvziok_name,c.period_code,
        c.reporting_period_text::bigint,
        (d.month_start+interval '1 month - 1 day')::date,
        make_date(right(c.period_code,4)::int,1,1), -- period 8: cumulative since January
        (d.month_start+interval '1 month - 1 day')::date,c.value,c.value_measure,
        c.run_id,c.raw_id,c.node_ordinal
    FROM bronze.v_inv_candidates c
    JOIN silver.dim_territory t ON t.territory_id=c.kato_id
    JOIN (SELECT DISTINCT member_id,member_name FROM bronze.v_taldau_tree_nodes
          WHERE run_id=p_run AND dimension='krp') k ON k.member_id=c.krp_id
    JOIN (SELECT DISTINCT member_id,member_name FROM bronze.v_taldau_tree_nodes
          WHERE run_id=p_run AND dimension='sif') s ON s.member_id=c.sif_id
    CROSS JOIN LATERAL (SELECT make_date(right(c.period_code,4)::int,left(c.period_code,2)::int,1) AS month_start) d
    WHERE c.run_id=p_run AND c.value IS NOT NULL;
    IF (SELECT count(*) FROM silver.inv_fixed_assets WHERE source_run_id=p_run) <> (r.scope->>'expected_numeric')::bigint THEN
        RAISE EXCEPTION 'Silver row loss during dimension joins';
    END IF;
    UPDATE bronze.extraction_runs SET status='silver_validated' WHERE run_id=p_run;
    RETURN result;
END $$;
