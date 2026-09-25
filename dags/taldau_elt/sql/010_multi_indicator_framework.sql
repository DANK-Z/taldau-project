-- Generic multi-indicator control, staging and publication framework.
-- Additive by design: investment-only objects remain available to legacy DAGs.
CREATE SCHEMA IF NOT EXISTS taldau;

-- 001 originally limited the raw traversal label to the four investment dimensions.
-- A Taldau dimension name is source metadata and must not be restricted by that pilot list.
ALTER TABLE taldau.bronze_taldau_api_raw
    DROP CONSTRAINT IF EXISTS bronze_taldau_api_raw_dimension_check;

CREATE TABLE IF NOT EXISTS taldau.metadata_indicator_registry (
    indicator_key text PRIMARY KEY CHECK (indicator_key ~ '^[a-z][a-z0-9_]{1,62}$'),
    display_name text NOT NULL,
    pipeline_type text NOT NULL CHECK (pipeline_type IN ('region_metric','cube')),
    indicator_id bigint,
    period_id integer,
    endpoint text,
    dimensions jsonb,
    roots jsonb,
    extraction_config jsonb,
    enabled boolean NOT NULL DEFAULT false,
    year_start integer NOT NULL DEFAULT 2023,
    year_end integer NOT NULL DEFAULT 2100,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (year_start BETWEEN 2000 AND 2100 AND year_end BETWEEN year_start AND 2100),
    CHECK (dimensions IS NULL OR jsonb_typeof(dimensions)='array'),
    CHECK (roots IS NULL OR jsonb_typeof(roots)='object'),
    CHECK (extraction_config IS NULL OR jsonb_typeof(extraction_config)='object'),
    CHECK (NOT enabled OR (
        indicator_id IS NOT NULL AND period_id IS NOT NULL AND endpoint IS NOT NULL
        AND dimensions IS NOT NULL AND roots IS NOT NULL AND extraction_config IS NOT NULL
        AND jsonb_array_length(dimensions)>0
        AND roots<>'{}'::jsonb
        AND extraction_config ? 'measure_id' AND extraction_config ? 'idx'
        AND extraction_config ? 'frequency' AND extraction_config ? 'period_code_regex'
        AND extraction_config ? 'expected_periods_per_year'
        AND coalesce(extraction_config->>'frequency' IN ('monthly','quarterly','annual'),false)
        AND coalesce(extraction_config->>'measure_id','') ~ '^[0-9]+$'
        AND coalesce(extraction_config->>'idx','') ~ '^[0-9]+$'
        AND coalesce(extraction_config->>'expected_periods_per_year','') ~ '^[1-9][0-9]*$'
        AND coalesce(extraction_config->>'period_code_regex','')<>''
    ))
);

INSERT INTO taldau.metadata_indicator_registry
    (indicator_key,display_name,pipeline_type,indicator_id,period_id,endpoint,dimensions,roots,
     extraction_config,enabled,year_start,year_end)
VALUES
('investments_fixed_assets','Инвестиции в основной капитал','cube',701827,8,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"kato","dic_id":68,"chunk":true},{"key":"krp","dic_id":90},{"key":"sif","dic_id":459},{"key":"gsvziok","dic_id":4043}]',
 '{"kato":"741880","krp":"741927","sif":"807855","gsvziok":"19202525"}',
 '{"strategy":"tree_cube","measure_id":1,"idx":3,"frequency":"monthly","period_code_regex":"^(0[1-9]|1[0-2])[0-9]{4}$","expected_periods_per_year":12}',
 true,2023,2100),
('population','Население','region_metric',NULL,NULL,NULL,NULL,NULL,NULL,false,2023,2100),
('average_salary','Среднемесячная заработная плата','region_metric',NULL,NULL,NULL,NULL,NULL,NULL,false,2023,2100),
('grp','Валовой региональный продукт','region_metric',NULL,NULL,NULL,NULL,NULL,NULL,false,2023,2100),
('agriculture','Сельское хозяйство','cube',NULL,NULL,NULL,NULL,NULL,NULL,false,2023,2100),
('industry','Промышленность','cube',NULL,NULL,NULL,NULL,NULL,NULL,false,2023,2100),
('trade','Торговля','cube',NULL,NULL,NULL,NULL,NULL,NULL,false,2023,2100),
('construction','Строительство','cube',NULL,NULL,NULL,NULL,NULL,NULL,false,2023,2100)
ON CONFLICT (indicator_key) DO NOTHING;

CREATE OR REPLACE VIEW taldau.metadata_enabled_indicators AS
SELECT r.*,
       extraction_config || jsonb_build_object(
         'indicator_key',indicator_key,'indicator_id',indicator_id,'period_id',period_id,
         'endpoint',endpoint,'dimensions',dimensions,'roots',roots,
         'dic_ids',(SELECT string_agg(d->>'dic_id',',' ORDER BY ord)
                    FROM jsonb_array_elements(dimensions) WITH ORDINALITY x(d,ord)),
         'measure_id',(extraction_config->>'measure_id')::integer,
         'idx',(extraction_config->>'idx')::integer,
         'frequency',extraction_config->>'frequency',
         'period_code_regex',extraction_config->>'period_code_regex',
         'expected_periods_per_year',(extraction_config->>'expected_periods_per_year')::integer,
         'strategy',extraction_config->>'strategy'
       ) AS source_config
FROM taldau.metadata_indicator_registry r
WHERE enabled
  AND NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(dimensions) d
    WHERE nullif(d->>'key','') IS NULL OR coalesce(d->>'dic_id','') !~ '^[0-9]+$'
       OR nullif(roots->>(d->>'key'),'') IS NULL
  );

INSERT INTO taldau.metadata_elt_pipelines(pipeline_id,pipeline_type,config)
SELECT 'statistics_'||indicator_key,pipeline_type,source_config
FROM taldau.metadata_enabled_indicators
ON CONFLICT(pipeline_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS taldau.bronze_batches (
    batch_id text PRIMARY KEY CHECK (batch_id ~ '^[A-Za-z0-9_-]{1,100}$'),
    year_start integer NOT NULL,
    year_end integer NOT NULL,
    state text NOT NULL DEFAULT 'prepared' CHECK
      (state IN ('prepared','loading','failed','validated','partially_validated','published')),
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    last_error text,
    CHECK (year_start BETWEEN 2000 AND 2100 AND year_end BETWEEN year_start AND 2100)
);

CREATE TABLE IF NOT EXISTS taldau.bronze_snapshots (
    snapshot_id text PRIMARY KEY CHECK (snapshot_id ~ '^[A-Za-z0-9_-]{1,160}$'),
    snapshot_revision bigint GENERATED ALWAYS AS IDENTITY,
    batch_id text REFERENCES taldau.bronze_batches,
    indicator_key text NOT NULL REFERENCES taldau.metadata_indicator_registry,
    source_config jsonb NOT NULL,
    year_start integer NOT NULL,
    year_end integer NOT NULL,
    reuse_snapshot_id text REFERENCES taldau.bronze_snapshots,
    discovery_run_id text NOT NULL UNIQUE REFERENCES taldau.bronze_extraction_runs,
    state text NOT NULL DEFAULT 'prepared' CHECK
      (state IN ('prepared','discovering','loading','failed','validated','published')),
    discovery_complete boolean NOT NULL DEFAULT false,
    expected_chunks integer,
    created_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    published_at timestamptz,
    last_error text,
    CHECK (year_start BETWEEN 2000 AND 2100 AND year_end BETWEEN year_start AND 2100),
    UNIQUE(batch_id,indicator_key)
);
CREATE INDEX IF NOT EXISTS bronze_snapshots_batch_state_idx
    ON taldau.bronze_snapshots(batch_id,state,indicator_key);

-- PostgreSQL now() is stable for the whole transaction, so created_at cannot
-- safely order snapshots created in one transaction. This also upgrades a
-- database where an earlier revision of migration 010 created the table.
ALTER TABLE taldau.bronze_snapshots
    ADD COLUMN IF NOT EXISTS snapshot_revision bigint GENERATED ALWAYS AS IDENTITY;
CREATE UNIQUE INDEX IF NOT EXISTS bronze_snapshots_revision_uidx
    ON taldau.bronze_snapshots(snapshot_revision);

CREATE TABLE IF NOT EXISTS taldau.bronze_chunks (
    chunk_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_id text NOT NULL REFERENCES taldau.bronze_snapshots,
    chunk_key text NOT NULL,
    coordinates jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(coordinates)='object'),
    run_id text NOT NULL UNIQUE REFERENCES taldau.bronze_extraction_runs,
    state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','running','complete','failed')),
    attempt integer NOT NULL DEFAULT 0,
    lease_token text,
    lease_until timestamptz,
    heartbeat_at timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    staged_at timestamptz,
    raw_count bigint,
    last_error text,
    UNIQUE(snapshot_id,chunk_key)
);
CREATE INDEX IF NOT EXISTS bronze_chunks_pending_idx
    ON taldau.bronze_chunks(snapshot_id,state,chunk_id);

CREATE TABLE IF NOT EXISTS taldau.bronze_request_tasks (
    run_id text NOT NULL REFERENCES taldau.bronze_extraction_runs,
    request_hash text NOT NULL,
    state text NOT NULL CHECK (state IN ('queued','complete')),
    dimension text NOT NULL,
    request_params jsonb NOT NULL,
    raw_id bigint REFERENCES taldau.bronze_taldau_api_raw,
    completed_at timestamptz,
    PRIMARY KEY(run_id,request_hash),
    CHECK ((state='complete')=(raw_id IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS taldau.bronze_snapshot_members (
    snapshot_id text NOT NULL REFERENCES taldau.bronze_snapshots,
    run_id text NOT NULL REFERENCES taldau.bronze_extraction_runs,
    raw_id bigint NOT NULL REFERENCES taldau.bronze_taldau_api_raw,
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
CREATE INDEX IF NOT EXISTS bronze_snapshot_members_lookup_idx
    ON taldau.bronze_snapshot_members(snapshot_id,dimension,member_id);

CREATE TABLE IF NOT EXISTS taldau.bronze_reuse_raw (
    snapshot_id text NOT NULL REFERENCES taldau.bronze_snapshots,
    request_hash text NOT NULL,
    raw_id bigint NOT NULL REFERENCES taldau.bronze_taldau_api_raw,
    PRIMARY KEY(snapshot_id,request_hash)
);

CREATE OR REPLACE VIEW taldau.bronze_run_raw AS
SELECT r.*,r.run_id AS source_run_id
FROM taldau.bronze_taldau_api_raw r
UNION ALL
SELECT r.id,q.run_id,r.indicator_id,r.endpoint,r.period_id,r.request_params,r.request_hash,
       r.response_data,r.response_text,r.response_hash,r.http_status,r.dimension,r.tree_depth,r.loaded_at,
       r.run_id AS source_run_id
FROM taldau.bronze_request_tasks q
JOIN taldau.bronze_taldau_api_raw r ON r.id=q.raw_id
WHERE q.state='complete' AND r.run_id<>q.run_id;

CREATE TABLE IF NOT EXISTS taldau.staging_observation_cells (
    snapshot_id text NOT NULL REFERENCES taldau.bronze_snapshots,
    chunk_id bigint NOT NULL REFERENCES taldau.bronze_chunks,
    run_id text NOT NULL REFERENCES taldau.bronze_extraction_runs,
    raw_id bigint NOT NULL REFERENCES taldau.bronze_taldau_api_raw,
    node_ordinal bigint NOT NULL,
    indicator_key text NOT NULL REFERENCES taldau.metadata_indicator_registry,
    indicator_id bigint,
    period_code text NOT NULL,
    reporting_period bigint,
    reporting_period_text text,
    coordinates jsonb NOT NULL CHECK (jsonb_typeof(coordinates)='object'),
    coordinate_hash text NOT NULL CHECK (length(coordinate_hash)=32),
    has_value boolean NOT NULL,
    has_period boolean NOT NULL,
    raw_value text,
    value numeric,
    value_status text NOT NULL CHECK (value_status IN ('numeric','x','missing','invalid')),
    value_measure text,
    PRIMARY KEY(snapshot_id,raw_id,node_ordinal,period_code)
);
CREATE INDEX IF NOT EXISTS staging_observation_chunk_idx ON taldau.staging_observation_cells(chunk_id);
CREATE INDEX IF NOT EXISTS staging_observation_grain_idx ON taldau.staging_observation_cells
    (snapshot_id,indicator_key,reporting_period,coordinate_hash);

CREATE TABLE IF NOT EXISTS taldau.quality_snapshot_checks (
    snapshot_id text NOT NULL REFERENCES taldau.bronze_snapshots,
    check_name text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('blocking','warning')),
    violations bigint NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(snapshot_id,check_name)
);
CREATE TABLE IF NOT EXISTS taldau.quality_period_diagnostics (
    snapshot_id text NOT NULL REFERENCES taldau.bronze_snapshots,
    period_code text NOT NULL,
    reporting_period bigint NOT NULL,
    numeric_rows bigint NOT NULL,
    x_rows bigint NOT NULL,
    invalid_rows bigint NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(snapshot_id,period_code,reporting_period)
);

CREATE TABLE IF NOT EXISTS taldau.silver_observations (
    indicator_key text NOT NULL REFERENCES taldau.metadata_indicator_registry,
    indicator_id bigint NOT NULL,
    reporting_period bigint NOT NULL,
    period_code text NOT NULL,
    period_start date,
    period_end date,
    coordinates jsonb NOT NULL CHECK (jsonb_typeof(coordinates)='object'),
    coordinate_hash text NOT NULL CHECK (length(coordinate_hash)=32),
    value numeric NOT NULL,
    value_measure text,
    source_snapshot_id text NOT NULL REFERENCES taldau.bronze_snapshots,
    source_run_id text NOT NULL REFERENCES taldau.bronze_extraction_runs,
    source_raw_id bigint NOT NULL REFERENCES taldau.bronze_taldau_api_raw,
    source_node_ordinal bigint NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(indicator_key,reporting_period,coordinate_hash),
    UNIQUE(indicator_key,reporting_period,coordinates)
);
CREATE INDEX IF NOT EXISTS silver_observations_snapshot_idx
    ON taldau.silver_observations(source_snapshot_id);

CREATE TABLE IF NOT EXISTS taldau.gold_dim_stat_indicator (
    indicator_key text PRIMARY KEY REFERENCES taldau.metadata_indicator_registry,
    indicator_id bigint NOT NULL,
    display_name text NOT NULL,
    pipeline_type text NOT NULL,
    frequency text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS taldau.gold_dim_member (
    member_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    indicator_key text NOT NULL REFERENCES taldau.metadata_indicator_registry,
    dimension text NOT NULL,
    source_id bigint NOT NULL,
    member_name text NOT NULL,
    parent_source_id bigint,
    source_tree_depth integer NOT NULL,
    is_total boolean NOT NULL,
    source_run_id text NOT NULL REFERENCES taldau.bronze_extraction_runs,
    UNIQUE(indicator_key,dimension,source_id)
);
CREATE TABLE IF NOT EXISTS taldau.gold_dim_period (
    indicator_key text NOT NULL REFERENCES taldau.metadata_indicator_registry,
    reporting_period bigint NOT NULL,
    period_code text NOT NULL,
    period_start date,
    period_end date,
    frequency text NOT NULL,
    PRIMARY KEY(indicator_key,reporting_period)
);
CREATE TABLE IF NOT EXISTS taldau.gold_fact_observations (
    indicator_key text NOT NULL,
    reporting_period bigint NOT NULL,
    coordinates jsonb NOT NULL CHECK (jsonb_typeof(coordinates)='object'),
    coordinate_hash text NOT NULL CHECK (length(coordinate_hash)=32),
    value numeric NOT NULL,
    value_measure text,
    source_snapshot_id text NOT NULL REFERENCES taldau.bronze_snapshots,
    source_run_id text NOT NULL REFERENCES taldau.bronze_extraction_runs,
    source_raw_id bigint NOT NULL REFERENCES taldau.bronze_taldau_api_raw,
    PRIMARY KEY(indicator_key,reporting_period,coordinate_hash),
    FOREIGN KEY(indicator_key,reporting_period)
      REFERENCES taldau.gold_dim_period(indicator_key,reporting_period),
    UNIQUE(indicator_key,reporting_period,coordinates)
);

CREATE INDEX IF NOT EXISTS gold_fact_observations_lookup_idx
    ON taldau.gold_fact_observations (indicator_key, coordinates, reporting_period) INCLUDE (value);

CREATE OR REPLACE FUNCTION taldau.generic_bigint(t text) RETURNS bigint
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN t ~ '^[0-9]{1,18}$' THEN t::bigint END
$$;

CREATE OR REPLACE FUNCTION taldau.generic_period_start(p_code text,p_frequency text) RETURNS date
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF p_frequency='monthly' AND p_code ~ '^(0[1-9]|1[0-2])[0-9]{4}$' THEN
    RETURN make_date(right(p_code,4)::int,left(p_code,2)::int,1);
  ELSIF p_frequency='quarterly' AND p_code ~ '^Q[1-4][0-9]{4}$' THEN
    RETURN make_date(right(p_code,4)::int,((substring(p_code,2,1)::int-1)*3)+1,1);
  ELSIF p_frequency='annual' AND p_code ~ '^[0-9]{4}$' THEN
    RETURN make_date(p_code::int,1,1);
  END IF;
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION taldau.generic_period_end(p_code text,p_frequency text) RETURNS date
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p_frequency
    WHEN 'monthly' THEN (taldau.generic_period_start(p_code,p_frequency)+interval '1 month - 1 day')::date
    WHEN 'quarterly' THEN (taldau.generic_period_start(p_code,p_frequency)+interval '3 months - 1 day')::date
    WHEN 'annual' THEN (taldau.generic_period_start(p_code,p_frequency)+interval '1 year - 1 day')::date
  END
$$;

CREATE OR REPLACE FUNCTION taldau.stage_snapshot_run(p_snapshot text,p_run text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM taldau.bronze_snapshot_members WHERE snapshot_id=p_snapshot AND run_id=p_run;
  INSERT INTO taldau.bronze_snapshot_members
  SELECT p_snapshot,r.run_id,r.id,n.ord,r.dimension,taldau.generic_bigint(n.node->>'id'),
         btrim(n.node->>'text'),taldau.generic_bigint(r.request_params->>'p_parent_id'),r.tree_depth,
         CASE lower(n.node->>'leaf') WHEN 'true' THEN true WHEN 'false' THEN false END,
         r.request_params->>'p_terms'
  FROM taldau.bronze_run_raw r
  CROSS JOIN LATERAL jsonb_array_elements(r.response_data) WITH ORDINALITY n(node,ord)
  WHERE r.run_id=p_run;
END $$;

CREATE OR REPLACE FUNCTION taldau.stage_observation_chunk(p_chunk bigint) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE c taldau.bronze_chunks; s taldau.bronze_snapshots; inserted bigint; dimension_count int;
BEGIN
  SELECT * INTO STRICT c FROM taldau.bronze_chunks WHERE chunk_id=p_chunk FOR UPDATE;
  SELECT * INTO STRICT s FROM taldau.bronze_snapshots WHERE snapshot_id=c.snapshot_id;
  IF c.state<>'running' OR s.state<>'loading' THEN RAISE EXCEPTION 'Chunk is not running'; END IF;
  SELECT jsonb_array_length(s.source_config->'dimensions') INTO dimension_count;
  PERFORM taldau.stage_snapshot_run(c.snapshot_id,c.run_id);
  DELETE FROM taldau.staging_observation_cells WHERE chunk_id=p_chunk;
  INSERT INTO taldau.staging_observation_cells
    (snapshot_id,chunk_id,run_id,raw_id,node_ordinal,indicator_key,indicator_id,period_code,
     reporting_period,reporting_period_text,coordinates,coordinate_hash,has_value,has_period,
     raw_value,value,value_status,value_measure)
  SELECT c.snapshot_id,c.chunk_id,r.run_id,r.id,n.ord,s.indicator_key,r.indicator_id,k.code,
         taldau.generic_bigint(n.node->>k.code),n.node->>k.code,coord.coordinates,
         md5(coord.coordinates::text),n.node ? ('y'||k.code),n.node ? k.code,
         n.node->>('y'||k.code),
         CASE WHEN n.node->>('y'||k.code) ~ '^[+-]?[0-9]+([.][0-9]+)?$'
              THEN (n.node->>('y'||k.code))::numeric END,
         CASE WHEN NOT (n.node ? ('y'||k.code)) OR NOT (n.node ? k.code) THEN 'missing'
              WHEN n.node->>('y'||k.code) ~ '^[+-]?[0-9]+([.][0-9]+)?$' THEN 'numeric'
              WHEN n.node->>('y'||k.code)='x' THEN 'x' ELSE 'invalid' END,
         n.node->>'measureName'
  FROM taldau.bronze_run_raw r
  CROSS JOIN LATERAL jsonb_array_elements(r.response_data) WITH ORDINALITY n(node,ord)
  CROSS JOIN LATERAL (
    SELECT DISTINCT regexp_replace(key,'^y','') AS code FROM jsonb_object_keys(n.node) key
    WHERE regexp_replace(key,'^y','') ~ (s.source_config->>'period_code_regex')
      AND taldau.generic_bigint(right(regexp_replace(key,'^y',''),4)) BETWEEN s.year_start AND s.year_end
  ) k
  CROSS JOIN LATERAL (
    SELECT jsonb_object_agg(d.item->>'key',
      CASE WHEN d.ord=dimension_count THEN n.node->>'id'
           ELSE split_part(r.request_params->>'p_terms',',',d.ord::int) END ORDER BY d.ord) AS coordinates
    FROM jsonb_array_elements(s.source_config->'dimensions') WITH ORDINALITY d(item,ord)
  ) coord
  WHERE ((dimension_count=1 AND r.run_id=s.discovery_run_id)
         OR (dimension_count>1 AND r.run_id=c.run_id))
    AND r.dimension=(s.source_config->'dimensions'->(dimension_count-1)->>'key')
    AND (dimension_count<>1 OR n.node->>'id'=c.coordinates->>(s.source_config->'dimensions'->0->>'key'));
  GET DIAGNOSTICS inserted=ROW_COUNT;
  UPDATE taldau.bronze_chunks SET staged_at=now() WHERE chunk_id=p_chunk;
  RETURN inserted;
END $$;

CREATE OR REPLACE FUNCTION taldau.validate_snapshot(p_snapshot text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE s taldau.bronze_snapshots; bad bigint; numeric_rows bigint; expected int; current_partial boolean;
BEGIN
  SELECT * INTO STRICT s FROM taldau.bronze_snapshots WHERE snapshot_id=p_snapshot FOR UPDATE;
  DELETE FROM taldau.quality_snapshot_checks WHERE snapshot_id=p_snapshot;
  INSERT INTO taldau.quality_snapshot_checks(snapshot_id,check_name,severity,violations,details)
  VALUES
   (p_snapshot,'discovery_complete','blocking',CASE WHEN s.discovery_complete THEN 0 ELSE 1 END,'{}'),
   (p_snapshot,'chunk_inventory','blocking',CASE WHEN s.expected_chunks=(SELECT count(*) FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot) THEN 0 ELSE 1 END,'{}'),
   (p_snapshot,'complete_chunks','blocking',(SELECT count(*) FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot AND state<>'complete'),'{}'),
   (p_snapshot,'failed_chunks','blocking',(SELECT count(*) FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot AND state='failed'),'{}'),
   (p_snapshot,'missing_request_checkpoints','blocking',(
      SELECT count(*) FROM taldau.bronze_run_raw r
      WHERE (r.run_id=s.discovery_run_id OR r.run_id IN (SELECT run_id FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot))
        AND NOT EXISTS (SELECT 1 FROM taldau.bronze_request_tasks q WHERE q.run_id=r.run_id AND q.raw_id=r.id AND q.state='complete')),'{}'),
   (p_snapshot,'incomplete_request_tasks','blocking',(
      SELECT count(*) FROM taldau.bronze_request_tasks q
      WHERE (q.run_id=s.discovery_run_id OR q.run_id IN (SELECT run_id FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot))
        AND (q.state<>'complete' OR q.raw_id IS NULL)),'{}'),
   (p_snapshot,'incomplete_tree_coverage','blocking',(
      SELECT count(*) FROM taldau.bronze_run_raw parent
      CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(parent.response_data)='array'
        THEN parent.response_data ELSE '[]'::jsonb END) node
      WHERE (parent.run_id=s.discovery_run_id OR parent.run_id IN (SELECT run_id FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot))
        AND lower(node->>'leaf')='false' AND NOT EXISTS (
          SELECT 1 FROM taldau.bronze_run_raw child WHERE child.run_id=parent.run_id
            AND child.dimension=parent.dimension AND child.request_params->>'p_parent_id'=node->>'id'
            AND (child.request_params-'p_parent_id')=(parent.request_params-'p_parent_id'))),'{}'),
   (p_snapshot,'malformed_responses','blocking',(
      SELECT count(*) FROM taldau.bronze_run_raw r
      WHERE (r.run_id=s.discovery_run_id OR r.run_id IN (SELECT run_id FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot))
        AND (jsonb_typeof(r.response_data)<>'array' OR EXISTS (
          SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(r.response_data)='array'
               THEN r.response_data ELSE '[]'::jsonb END) n
          WHERE taldau.generic_bigint(n->>'id') IS NULL OR coalesce(btrim(n->>'text'),'')=''
             OR lower(coalesce(n->>'leaf','')) NOT IN ('true','false')))),'{}'),
   (p_snapshot,'duplicate_request_versions','blocking',(
      SELECT count(*) FROM (SELECT request_hash FROM taldau.bronze_run_raw
       WHERE run_id=s.discovery_run_id OR run_id IN (SELECT run_id FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot)
       GROUP BY request_hash HAVING count(DISTINCT (endpoint,request_params,response_hash))>1) q),'{}'),
   (p_snapshot,'raw_config_mismatch','blocking',(
      SELECT count(*) FROM taldau.bronze_run_raw r
      WHERE (r.run_id=s.discovery_run_id OR r.run_id IN (SELECT run_id FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot))
        AND (r.indicator_id IS DISTINCT FROM (s.source_config->>'indicator_id')::bigint
          OR r.period_id IS DISTINCT FROM (s.source_config->>'period_id')::integer
          OR r.endpoint IS DISTINCT FROM s.source_config->>'endpoint'
          OR r.request_params->>'p_index_id' IS DISTINCT FROM s.source_config->>'indicator_id'
          OR r.request_params->>'p_period_id' IS DISTINCT FROM s.source_config->>'period_id')),'{}'),
   (p_snapshot,'duplicate_fact_grain','blocking',(
      SELECT count(*) FROM (SELECT indicator_key,reporting_period,coordinates FROM taldau.staging_observation_cells
       WHERE snapshot_id=p_snapshot AND value_status IN ('numeric','x') GROUP BY 1,2,3 HAVING count(*)>1) q),'{}'),
   (p_snapshot,'null_required_keys','blocking',(
      SELECT count(*) FROM taldau.staging_observation_cells v
      WHERE snapshot_id=p_snapshot AND (indicator_id IS NULL OR reporting_period IS NULL
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(s.source_config->'dimensions') d
                   WHERE nullif(v.coordinates->>(d->>'key'),'') IS NULL
                      OR taldau.generic_bigint(v.coordinates->>(d->>'key')) IS NULL))),'{}'),
   (p_snapshot,'missing_dimension_members','blocking',(
      SELECT count(*) FROM taldau.staging_observation_cells v
      CROSS JOIN LATERAL jsonb_each_text(v.coordinates) d(dimension,member_id)
      WHERE v.snapshot_id=p_snapshot AND v.value_status IN ('numeric','x') AND NOT EXISTS (
        SELECT 1 FROM taldau.bronze_snapshot_members m WHERE m.snapshot_id=p_snapshot
          AND m.dimension=d.dimension AND m.member_id=taldau.generic_bigint(d.member_id))),'{}'),
   (p_snapshot,'unknown_non_numeric','blocking',(
      SELECT count(*) FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot AND value_status='invalid'),'{}'),
   (p_snapshot,'orphan_period_value_pairs','blocking',(
      SELECT count(*) FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot AND value_status='missing'),'{}'),
   (p_snapshot,'period_year_consistency','blocking',(
      SELECT count(*) FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot
       AND (period_code !~ (s.source_config->>'period_code_regex')
         OR taldau.generic_bigint(right(period_code,4)) NOT BETWEEN s.year_start AND s.year_end)),'{}'),
   (p_snapshot,'conflicting_period_mapping','blocking',(
      SELECT count(*) FROM (
        SELECT period_code FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot
          AND reporting_period IS NOT NULL GROUP BY period_code HAVING count(DISTINCT reporting_period)>1
        UNION ALL
        SELECT reporting_period::text FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot
          AND reporting_period IS NOT NULL GROUP BY reporting_period HAVING count(DISTINCT period_code)>1
      ) q),'{}'),
   (p_snapshot,'conflicting_hierarchy','blocking',(
      SELECT count(*) FROM (SELECT dimension,member_id FROM taldau.bronze_snapshot_members
       WHERE snapshot_id=p_snapshot GROUP BY 1,2 HAVING count(DISTINCT (member_name,parent_id,tree_depth))>1) q),'{}'),
   (p_snapshot,'empty_snapshot','blocking',CASE WHEN EXISTS(
      SELECT 1 FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot AND value_status='numeric') THEN 0 ELSE 1 END,'{}');

  DELETE FROM taldau.quality_period_diagnostics WHERE snapshot_id=p_snapshot;
  INSERT INTO taldau.quality_period_diagnostics(snapshot_id,period_code,reporting_period,numeric_rows,x_rows,invalid_rows)
  SELECT p_snapshot,period_code,coalesce(reporting_period,-1),
         count(*) FILTER(WHERE value_status='numeric'),count(*) FILTER(WHERE value_status='x'),
         count(*) FILTER(WHERE value_status IN ('invalid','missing'))
  FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot
  GROUP BY period_code,coalesce(reporting_period,-1);

  expected:=coalesce((s.source_config->>'expected_periods_per_year')::int,0);
  current_partial:=s.year_end>=extract(year FROM current_date)::int;
  INSERT INTO taldau.quality_snapshot_checks(snapshot_id,check_name,severity,violations,details)
  SELECT p_snapshot,'missing_periods','warning',coalesce(sum(greatest(expected-periods,0)),0),
         jsonb_build_object('frequency',s.source_config->>'frequency','current_year_partial_allowed',current_partial)
  FROM (
    SELECT y,count(DISTINCT d.period_code)::int periods
    FROM generate_series(s.year_start,s.year_end) y
    LEFT JOIN taldau.quality_period_diagnostics d ON d.snapshot_id=p_snapshot AND right(d.period_code,4)=y::text
    WHERE y<extract(year FROM current_date)::int
    GROUP BY y
  ) years;

  SELECT coalesce(sum(violations),0) INTO bad FROM taldau.quality_snapshot_checks
  WHERE snapshot_id=p_snapshot AND severity='blocking';
  SELECT count(*) INTO numeric_rows FROM taldau.staging_observation_cells
  WHERE snapshot_id=p_snapshot AND value_status='numeric';
  UPDATE taldau.bronze_snapshots SET state=CASE WHEN bad=0 THEN 'validated' ELSE 'failed' END,
      validated_at=CASE WHEN bad=0 THEN now() ELSE NULL END,
      last_error=CASE WHEN bad=0 THEN NULL ELSE 'SQL validation failed: see taldau.quality_snapshot_checks' END
  WHERE snapshot_id=p_snapshot;
  RETURN jsonb_build_object('snapshot_id',p_snapshot,'valid',bad=0,'violations',bad,'numeric_rows',numeric_rows);
END $$;

CREATE OR REPLACE FUNCTION taldau.publish_snapshot(p_snapshot text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE s taldau.bronze_snapshots; result jsonb; inserted bigint; gold_rows bigint; frequency text;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('taldau.generic_publication',0));
  SELECT * INTO STRICT s FROM taldau.bronze_snapshots WHERE snapshot_id=p_snapshot FOR UPDATE;
  result:=taldau.validate_snapshot(p_snapshot);
  IF NOT (result->>'valid')::boolean THEN RAISE EXCEPTION 'Snapshot is incomplete/invalid: %',result; END IF;
  frequency:=s.source_config->>'frequency';
  LOCK TABLE taldau.silver_observations,taldau.gold_fact_observations IN SHARE ROW EXCLUSIVE MODE;
  IF EXISTS (
      SELECT 1
      FROM taldau.bronze_snapshots newer
      WHERE newer.indicator_key=s.indicator_key
        AND newer.state='published'
        AND newer.snapshot_id<>p_snapshot
        AND newer.snapshot_revision>s.snapshot_revision
        AND newer.year_start<=s.year_end
        AND newer.year_end>=s.year_start
  ) THEN
    RAISE EXCEPTION 'Newer data is already published for this indicator/year scope';
  END IF;
  DELETE FROM taldau.silver_observations
  WHERE indicator_key=s.indicator_key
    AND taldau.generic_bigint(right(period_code,4)) BETWEEN s.year_start AND s.year_end;
  INSERT INTO taldau.silver_observations
    (indicator_key,indicator_id,reporting_period,period_code,period_start,period_end,coordinates,
     coordinate_hash,value,value_measure,source_snapshot_id,source_run_id,source_raw_id,source_node_ordinal)
  SELECT indicator_key,indicator_id,reporting_period,period_code,
         taldau.generic_period_start(period_code,frequency),taldau.generic_period_end(period_code,frequency),
         coordinates,coordinate_hash,value,value_measure,p_snapshot,run_id,raw_id,node_ordinal
  FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot AND value_status='numeric';
  GET DIAGNOSTICS inserted=ROW_COUNT;
  IF inserted<>(result->>'numeric_rows')::bigint THEN RAISE EXCEPTION 'Silver publication lost rows'; END IF;

  INSERT INTO taldau.gold_dim_stat_indicator(indicator_key,indicator_id,display_name,pipeline_type,frequency)
  SELECT r.indicator_key,r.indicator_id,r.display_name,r.pipeline_type,frequency
  FROM taldau.metadata_indicator_registry r WHERE r.indicator_key=s.indicator_key
  ON CONFLICT(indicator_key) DO UPDATE SET indicator_id=excluded.indicator_id,display_name=excluded.display_name,
      pipeline_type=excluded.pipeline_type,frequency=excluded.frequency,updated_at=now();
  INSERT INTO taldau.gold_dim_member
    (indicator_key,dimension,source_id,member_name,parent_source_id,source_tree_depth,is_total,source_run_id)
  SELECT DISTINCT ON(m.dimension,m.member_id) s.indicator_key,m.dimension,m.member_id,m.member_name,m.parent_id,m.tree_depth,
         m.member_id::text=s.source_config->'roots'->>m.dimension,m.run_id
  FROM taldau.bronze_snapshot_members m WHERE m.snapshot_id=p_snapshot AND m.member_id IS NOT NULL
  ORDER BY m.dimension,m.member_id,m.raw_id,m.node_ordinal
  ON CONFLICT(indicator_key,dimension,source_id) DO UPDATE SET member_name=excluded.member_name,
      parent_source_id=excluded.parent_source_id,source_tree_depth=excluded.source_tree_depth,
      is_total=excluded.is_total,source_run_id=excluded.source_run_id;
  INSERT INTO taldau.gold_dim_period(indicator_key,reporting_period,period_code,period_start,period_end,frequency)
  SELECT DISTINCT indicator_key,reporting_period,period_code,period_start,period_end,frequency
  FROM taldau.silver_observations WHERE source_snapshot_id=p_snapshot
  ON CONFLICT(indicator_key,reporting_period) DO UPDATE SET period_code=excluded.period_code,
      period_start=excluded.period_start,period_end=excluded.period_end,frequency=excluded.frequency;
  DELETE FROM taldau.gold_fact_observations
  WHERE indicator_key=s.indicator_key AND (indicator_key,reporting_period) IN (
    SELECT indicator_key,reporting_period FROM taldau.gold_dim_period
    WHERE indicator_key=s.indicator_key
      AND taldau.generic_bigint(right(period_code,4)) BETWEEN s.year_start AND s.year_end);
  INSERT INTO taldau.gold_fact_observations
    (indicator_key,reporting_period,coordinates,coordinate_hash,value,value_measure,
     source_snapshot_id,source_run_id,source_raw_id)
  SELECT indicator_key,reporting_period,coordinates,coordinate_hash,value,value_measure,
         source_snapshot_id,source_run_id,source_raw_id
  FROM taldau.silver_observations WHERE source_snapshot_id=p_snapshot;
  GET DIAGNOSTICS gold_rows=ROW_COUNT;
  IF gold_rows<>inserted THEN RAISE EXCEPTION 'Gold publication lost rows: % != %',gold_rows,inserted; END IF;
  IF EXISTS ((SELECT indicator_key,reporting_period,coordinates,value FROM taldau.silver_observations WHERE source_snapshot_id=p_snapshot
              EXCEPT SELECT indicator_key,reporting_period,coordinates,value FROM taldau.gold_fact_observations WHERE source_snapshot_id=p_snapshot)
             UNION ALL
             (SELECT indicator_key,reporting_period,coordinates,value FROM taldau.gold_fact_observations WHERE source_snapshot_id=p_snapshot
              EXCEPT SELECT indicator_key,reporting_period,coordinates,value FROM taldau.silver_observations WHERE source_snapshot_id=p_snapshot)) THEN
    RAISE EXCEPTION 'Gold/Silver snapshot mismatch';
  END IF;
  UPDATE taldau.bronze_snapshots SET state='published',published_at=now() WHERE snapshot_id=p_snapshot;
  UPDATE taldau.bronze_batches b SET state='published',completed_at=now()
  WHERE b.batch_id=s.batch_id AND NOT EXISTS (
    SELECT 1 FROM taldau.bronze_snapshots x WHERE x.batch_id=b.batch_id AND x.state<>'published');
  RETURN inserted;
END $$;

-- Preserve the prepared production snapshot (and any local investment snapshots) exactly.
INSERT INTO taldau.bronze_batches(batch_id,year_start,year_end,state,created_at)
SELECT CASE WHEN snapshot_id='kz-investments-2023-2026-prod-v1'
            THEN 'taldau-statistics-2023-2026-prod-v1'
            ELSE 'legacy-'||left(md5(snapshot_id),24) END,
       coalesce(year_start,year),coalesce(year_end,year),
       CASE state WHEN 'published' THEN 'published' WHEN 'validated' THEN 'validated'
                  WHEN 'failed' THEN 'failed' ELSE 'prepared' END,created_at
FROM taldau.bronze_inv_snapshots
ON CONFLICT(batch_id) DO NOTHING;

INSERT INTO taldau.bronze_snapshots
 (snapshot_id,batch_id,indicator_key,source_config,year_start,year_end,reuse_snapshot_id,
  discovery_run_id,state,discovery_complete,expected_chunks,created_at,validated_at,published_at,last_error)
SELECT i.snapshot_id,CASE WHEN i.snapshot_id='kz-investments-2023-2026-prod-v1'
         THEN 'taldau-statistics-2023-2026-prod-v1'
         ELSE 'legacy-'||left(md5(i.snapshot_id),24) END,'investments_fixed_assets',
       i.config || jsonb_build_object(
         'indicator_key','investments_fixed_assets','dimensions',r.dimensions,
         'frequency',r.extraction_config->>'frequency',
         'period_code_regex',r.extraction_config->>'period_code_regex',
         'expected_periods_per_year',(r.extraction_config->>'expected_periods_per_year')::int,
         'strategy',r.extraction_config->>'strategy'),
       coalesce(i.year_start,i.year),coalesce(i.year_end,i.year),i.reuse_snapshot_id,
       i.discovery_run_id,i.state,i.discovery_complete,i.expected_chunks,i.created_at,
       i.validated_at,i.published_at,i.last_error
FROM taldau.bronze_inv_snapshots i
JOIN taldau.metadata_indicator_registry r ON r.indicator_key='investments_fixed_assets'
ON CONFLICT(snapshot_id) DO NOTHING;

-- Prepared snapshots have no children. These copies also preserve an accidentally started legacy run.
INSERT INTO taldau.bronze_chunks(snapshot_id,chunk_key,coordinates,run_id,state,attempt,lease_token,
 lease_until,heartbeat_at,started_at,completed_at,staged_at,raw_count,last_error)
SELECT c.snapshot_id,'kato:'||c.territory_id,jsonb_build_object('kato',c.territory_id::text),c.run_id,c.state,
 c.attempt,c.lease_token,c.lease_until,c.heartbeat_at,c.started_at,c.completed_at,c.staged_at,c.raw_count,c.last_error
FROM taldau.bronze_inv_chunks c JOIN taldau.bronze_snapshots s USING(snapshot_id)
ON CONFLICT(snapshot_id,chunk_key) DO NOTHING;

INSERT INTO taldau.bronze_request_tasks
SELECT q.* FROM taldau.bronze_inv_request_tasks q
WHERE q.run_id IN (SELECT discovery_run_id FROM taldau.bronze_snapshots UNION SELECT run_id FROM taldau.bronze_chunks)
ON CONFLICT(run_id,request_hash) DO NOTHING;

INSERT INTO taldau.bronze_snapshot_members
SELECT m.* FROM taldau.bronze_inv_snapshot_members m JOIN taldau.bronze_snapshots s USING(snapshot_id)
ON CONFLICT(snapshot_id,raw_id,node_ordinal) DO NOTHING;

INSERT INTO taldau.bronze_reuse_raw
SELECT r.* FROM taldau.bronze_inv_reuse_raw r JOIN taldau.bronze_snapshots s USING(snapshot_id)
ON CONFLICT(snapshot_id,request_hash) DO NOTHING;

INSERT INTO taldau.staging_observation_cells
 (snapshot_id,chunk_id,run_id,raw_id,node_ordinal,indicator_key,indicator_id,period_code,
  reporting_period,reporting_period_text,coordinates,coordinate_hash,has_value,has_period,
  raw_value,value,value_status,value_measure)
SELECT v.snapshot_id,c.chunk_id,v.run_id,v.raw_id,v.node_ordinal,'investments_fixed_assets',v.indicator_id,
       v.period_code,v.reporting_period,v.reporting_period_text,coord.coordinates,md5(coord.coordinates::text),
       v.has_value,v.has_period,v.raw_value,v.value,
       CASE WHEN NOT v.has_value OR NOT v.has_period THEN 'missing'
            WHEN v.value IS NOT NULL THEN 'numeric' WHEN v.raw_value='x' THEN 'x' ELSE 'invalid' END,
       v.value_measure
FROM taldau.staging_inv_year_cells v
JOIN taldau.bronze_chunks c ON c.snapshot_id=v.snapshot_id AND c.chunk_key='kato:'||v.kato_id
CROSS JOIN LATERAL (SELECT jsonb_build_object('kato',v.kato_id::text,'krp',v.krp_id::text,
  'sif',v.sif_id::text,'gsvziok',v.gsvziok_id::text) AS coordinates) coord
ON CONFLICT(snapshot_id,raw_id,node_ordinal,period_code) DO NOTHING;

INSERT INTO taldau.silver_observations
 (indicator_key,indicator_id,reporting_period,period_code,period_start,period_end,coordinates,
  coordinate_hash,value,value_measure,source_snapshot_id,source_run_id,source_raw_id,source_node_ordinal,loaded_at)
SELECT 'investments_fixed_assets',v.indicator_id,v.reporting_period,v.period_code,v.start_date,v.end_date,
       coord.coordinates,md5(coord.coordinates::text),v.value,v.value_measure,v.source_snapshot_id,
       v.source_run_id,v.source_raw_id,v.source_node_ordinal,v.loaded_at
FROM taldau.silver_inv_fixed_assets v
JOIN taldau.bronze_snapshots s ON s.snapshot_id=v.source_snapshot_id
CROSS JOIN LATERAL (SELECT jsonb_build_object('kato',v.kato_id::text,'krp',v.krp_id::text,
  'sif',v.sif_id::text,'gsvziok',v.gsvziok_id::text) AS coordinates) coord
ON CONFLICT(indicator_key,reporting_period,coordinate_hash) DO NOTHING;

COMMENT ON TABLE taldau.metadata_indicator_registry IS
 'Authoritative source registry. Only enabled rows satisfying the full configuration CHECK are scheduled.';
COMMENT ON TABLE taldau.staging_observation_cells IS
 'Lossless typed projection of period/value pairs; arbitrary source coordinates remain in JSONB.';
COMMENT ON FUNCTION taldau.publish_snapshot(text) IS
 'Explicit-only atomic Silver+Gold publication for one validated indicator snapshot and year scope.';
