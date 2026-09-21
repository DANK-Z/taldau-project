CREATE SCHEMA IF NOT EXISTS taldau;

-- Separate configuration: the legacy indicator 701827 uses a different cube.
CREATE TABLE IF NOT EXISTS taldau.metadata_elt_pipelines (
    pipeline_id text PRIMARY KEY,
    pipeline_type text NOT NULL CHECK (pipeline_type IN ('region_metric', 'cube')),
    config jsonb NOT NULL
);
INSERT INTO taldau.metadata_elt_pipelines VALUES ('inv_fixed_assets_monthly', 'cube',
'{"indicator_id":701827,"period_id":8,"endpoint":"https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData","dic_ids":"68,90,459,4043","roots":{"kato":"741880","krp":"741927","sif":"807855","gsvziok":"19202525"},"measure_id":1,"idx":3}')
ON CONFLICT (pipeline_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS taldau.bronze_extraction_runs (
    run_id text PRIMARY KEY,
    pipeline_id text NOT NULL REFERENCES taldau.metadata_elt_pipelines,
    config jsonb NOT NULL,
    scope jsonb NOT NULL,
    status text NOT NULL DEFAULT 'loading'
        CHECK (status IN ('loading','failed','bronze_complete','silver_validated','gold_validated')),
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    last_error text
);
CREATE TABLE IF NOT EXISTS taldau.bronze_taldau_api_raw (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id text NOT NULL REFERENCES taldau.bronze_extraction_runs,
    indicator_id bigint NOT NULL,
    endpoint text NOT NULL,
    period_id integer NOT NULL,
    request_params jsonb NOT NULL,
    request_hash text NOT NULL CHECK (length(request_hash)=64),
    response_data jsonb NOT NULL,
    response_text text NOT NULL,
    response_hash text NOT NULL CHECK (length(response_hash)=64),
    http_status integer NOT NULL CHECK (http_status BETWEEN 200 AND 299),
    -- Traversal context is metadata, never a replacement for the raw response.
    dimension text NOT NULL CHECK (dimension IN ('kato','krp','sif','gsvziok')),
    tree_depth integer NOT NULL CHECK (tree_depth>=0),
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (run_id, request_hash),
    CHECK (response_data = response_text::jsonb)
);
CREATE INDEX IF NOT EXISTS bronze_taldau_raw_run_dimension_idx
    ON taldau.bronze_taldau_api_raw (run_id, dimension);
COMMENT ON COLUMN taldau.bronze_taldau_api_raw.response_text IS
    'Original decoded HTTP body. JSONB preserves values/types but normalizes object formatting.';
COMMENT ON COLUMN taldau.bronze_taldau_api_raw.request_hash IS
    'SHA256 of canonical endpoint + exact string request parameters. Same run resumes; a new run captures a new source version.';
