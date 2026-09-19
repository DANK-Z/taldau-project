-- Additive migration. Existing pilot and region_metric objects are retained.
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quality;

CREATE TABLE IF NOT EXISTS bronze.inv_snapshots (
    snapshot_id text PRIMARY KEY,
    year integer NOT NULL CHECK (year=2025),
    config jsonb NOT NULL,
    discovery_run_id text NOT NULL UNIQUE REFERENCES bronze.extraction_runs,
    state text NOT NULL DEFAULT 'prepared' CHECK
        (state IN ('prepared','discovering','loading','failed','validated','published')),
    discovery_complete boolean NOT NULL DEFAULT false,
    expected_chunks integer,
    created_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    published_at timestamptz,
    last_error text
);
CREATE TABLE IF NOT EXISTS bronze.inv_chunks (
    chunk_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_id text NOT NULL REFERENCES bronze.inv_snapshots,
    territory_id bigint NOT NULL,
    run_id text NOT NULL UNIQUE REFERENCES bronze.extraction_runs,
    state text NOT NULL DEFAULT 'queued' CHECK(state IN ('queued','running','complete','failed')),
    attempt integer NOT NULL DEFAULT 0,
    lease_token text,
    lease_until timestamptz,
    heartbeat_at timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    staged_at timestamptz,
    raw_count bigint,
    last_error text,
    UNIQUE(snapshot_id,territory_id)
);
CREATE INDEX IF NOT EXISTS inv_chunks_pending_idx ON bronze.inv_chunks(snapshot_id,state,chunk_id);

-- Request checkpoint: written before HTTP; completed only after durable raw storage.
CREATE TABLE IF NOT EXISTS bronze.inv_request_tasks (
    run_id text NOT NULL REFERENCES bronze.extraction_runs,
    request_hash text NOT NULL,
    state text NOT NULL CHECK(state IN ('queued','complete')),
    dimension text NOT NULL,
    request_params jsonb NOT NULL,
    raw_id bigint REFERENCES bronze.taldau_api_raw,
    completed_at timestamptz,
    PRIMARY KEY(run_id,request_hash),
    CHECK ((state='complete')=(raw_id IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS bronze.inv_snapshot_members (
    snapshot_id text NOT NULL REFERENCES bronze.inv_snapshots,
    run_id text NOT NULL REFERENCES bronze.extraction_runs,
    raw_id bigint NOT NULL REFERENCES bronze.taldau_api_raw,
    node_ordinal bigint NOT NULL,
    dimension text NOT NULL,
    member_id bigint,
    member_name text,
    parent_id bigint,
    tree_depth integer NOT NULL,
    leaf boolean,
    terms text NOT NULL,
    PRIMARY KEY(snapshot_id,raw_id,node_ordinal)
);
CREATE INDEX IF NOT EXISTS inv_members_lookup_idx
    ON bronze.inv_snapshot_members(snapshot_id,dimension,member_id);
CREATE INDEX IF NOT EXISTS inv_members_run_idx ON bronze.inv_snapshot_members(run_id);

-- Includes x and invalid values for diagnostics. No natural-key uniqueness constraint here:
-- duplicates must be detected explicitly, not discarded with ON CONFLICT.
CREATE TABLE IF NOT EXISTS staging.inv_year_cells (
    snapshot_id text NOT NULL REFERENCES bronze.inv_snapshots,
    chunk_id bigint NOT NULL REFERENCES bronze.inv_chunks,
    run_id text NOT NULL REFERENCES bronze.extraction_runs,
    raw_id bigint NOT NULL REFERENCES bronze.taldau_api_raw,
    node_ordinal bigint NOT NULL,
    indicator_id bigint,
    kato_id bigint,krp_id bigint,sif_id bigint,gsvziok_id bigint,
    period_code text NOT NULL,
    reporting_period bigint,
    reporting_period_text text,
    has_value boolean NOT NULL,
    has_period boolean NOT NULL,
    raw_value text,
    value numeric,
    value_measure text,
    PRIMARY KEY(snapshot_id,raw_id,node_ordinal,period_code)
);
CREATE INDEX IF NOT EXISTS inv_cells_chunk_idx ON staging.inv_year_cells(chunk_id);
CREATE INDEX IF NOT EXISTS inv_cells_grain_idx ON staging.inv_year_cells
    (snapshot_id,indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id);

ALTER TABLE silver.inv_fixed_assets ADD COLUMN IF NOT EXISTS source_snapshot_id text
    REFERENCES bronze.inv_snapshots;
CREATE INDEX IF NOT EXISTS inv_silver_snapshot_idx ON silver.inv_fixed_assets(source_snapshot_id);

CREATE TABLE IF NOT EXISTS quality.inv_month_expectations (
    year integer NOT NULL,period_code text NOT NULL,reporting_period bigint NOT NULL,expected_rows bigint NOT NULL,
    PRIMARY KEY(year,period_code)
);
INSERT INTO quality.inv_month_expectations VALUES
 (2025,'012025',1071,22985),(2025,'022025',1076,33174),(2025,'032025',1077,41240),
 (2025,'042025',1078,48565),(2025,'052025',1079,53019),(2025,'062025',1080,58159),
 (2025,'072025',1075,60380),(2025,'082025',1074,62610),(2025,'092025',1073,65150),
 (2025,'102025',1072,66747),(2025,'112025',1070,68181),(2025,'122025',1069,71155)
ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS quality.inv_snapshot_checks (
    snapshot_id text NOT NULL REFERENCES bronze.inv_snapshots,
    check_name text NOT NULL,violations bigint NOT NULL,checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(snapshot_id,check_name)
);
CREATE TABLE IF NOT EXISTS quality.inv_month_diagnostics (
    snapshot_id text NOT NULL REFERENCES bronze.inv_snapshots,
    period_code text NOT NULL,reporting_period bigint NOT NULL,
    numeric_rows bigint NOT NULL,x_rows bigint NOT NULL,invalid_rows bigint NOT NULL,
    expected_rows bigint,delta bigint,checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(snapshot_id,period_code,reporting_period)
);

CREATE OR REPLACE FUNCTION bronze.inv_int(t text) RETURNS bigint
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN t ~ '^[0-9]{1,18}$' THEN t::bigint END
$$;

CREATE OR REPLACE FUNCTION bronze.stage_inv_run(p_snapshot text,p_run text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM bronze.inv_snapshot_members WHERE snapshot_id=p_snapshot AND run_id=p_run;
    INSERT INTO bronze.inv_snapshot_members
    SELECT p_snapshot,r.run_id,r.id,n.ord,r.dimension,bronze.inv_int(n.node->>'id'),
        btrim(n.node->>'text'),bronze.inv_int(r.request_params->>'p_parent_id'),r.tree_depth,
        CASE lower(n.node->>'leaf') WHEN 'true' THEN true WHEN 'false' THEN false END,
        r.request_params->>'p_terms'
    FROM bronze.taldau_api_raw r
    CROSS JOIN LATERAL jsonb_array_elements(r.response_data) WITH ORDINALITY n(node,ord)
    WHERE r.run_id=p_run;
END $$;

CREATE OR REPLACE FUNCTION staging.stage_inv_chunk(p_chunk bigint) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE c bronze.inv_chunks; s bronze.inv_snapshots; inserted bigint;
BEGIN
    SELECT * INTO STRICT c FROM bronze.inv_chunks WHERE chunk_id=p_chunk FOR UPDATE;
    SELECT * INTO STRICT s FROM bronze.inv_snapshots WHERE snapshot_id=c.snapshot_id;
    IF c.state<>'running' OR s.state<>'loading' THEN RAISE EXCEPTION 'Chunk is not running'; END IF;
    IF (SELECT status FROM bronze.extraction_runs WHERE run_id=c.run_id)<>'bronze_complete' THEN
        RAISE EXCEPTION 'Traversal is not finished';
    END IF;
    PERFORM bronze.stage_inv_run(c.snapshot_id,c.run_id);
    DELETE FROM staging.inv_year_cells WHERE chunk_id=p_chunk;
    INSERT INTO staging.inv_year_cells
    SELECT c.snapshot_id,c.chunk_id,r.run_id,r.id,n.ord,r.indicator_id,
        bronze.inv_int(split_part(r.request_params->>'p_terms',',',1)),
        bronze.inv_int(split_part(r.request_params->>'p_terms',',',2)),
        bronze.inv_int(split_part(r.request_params->>'p_terms',',',3)),
        bronze.inv_int(n.node->>'id'),k.code,
        bronze.inv_int(n.node->>k.code),n.node->>k.code,
        n.node ? ('y'||k.code),n.node ? k.code,n.node->>('y'||k.code),
        CASE WHEN n.node->>('y'||k.code) ~ '^[+-]?[0-9]+([.][0-9]+)?$'
             THEN (n.node->>('y'||k.code))::numeric END,n.node->>'measureName'
    FROM bronze.taldau_api_raw r
    CROSS JOIN LATERAL jsonb_array_elements(r.response_data) WITH ORDINALITY n(node,ord)
    CROSS JOIN LATERAL (
        -- Union of observed value/period keys also exposes orphan pairs to validation.
        SELECT DISTINCT regexp_replace(key,'^y','') AS code FROM jsonb_object_keys(n.node) key
        WHERE key ~ ('^y?[0-9]{2}'||s.year::text||'$')
    ) k
    WHERE r.run_id=c.run_id AND r.dimension='gsvziok';
    GET DIAGNOSTICS inserted=ROW_COUNT;
    UPDATE bronze.inv_chunks SET staged_at=now() WHERE chunk_id=p_chunk;
    RETURN inserted;
END $$;
