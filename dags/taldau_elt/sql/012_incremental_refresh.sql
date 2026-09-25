-- Generic bounded refresh. Safe to replay; frozen snapshots and legacy tables are untouched.
CREATE SCHEMA IF NOT EXISTS taldau;

ALTER TABLE taldau.metadata_indicator_registry ALTER COLUMN year_end SET DEFAULT 2100;
UPDATE taldau.metadata_indicator_registry SET year_end=2100,updated_at=now()
WHERE enabled AND year_end=2026 AND indicator_key IN
 ('agriculture','average_salary','construction','grp','industry',
  'investments_fixed_assets','population','trade');

-- Persistent ownership covers all continuation waves, including failed runs.
CREATE TABLE IF NOT EXISTS taldau.metadata_batch_owners (
    owner_key text PRIMARY KEY,
    batch_id text NOT NULL REFERENCES taldau.bronze_batches,
    acquired_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gold_fact_observations_lookup_idx
    ON taldau.gold_fact_observations (indicator_key, coordinates, reporting_period) INCLUDE (value);

-- The API embeds all years in each response. Keep Bronze lossless, project only
-- source-present periods in scope, frozen at the original run's local date.
-- Population's annual 12YYYY label denotes the start of that year.
CREATE OR REPLACE FUNCTION taldau.generic_period_available(p_code text,p_config jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN NOT (p_config ? 'incremental_as_of') THEN true
    WHEN p_config->>'period_semantics'='point_in_time_start_period'
      THEN taldau.generic_period_start(p_code,p_config->>'frequency') <= (p_config->>'incremental_as_of')::date
    ELSE taldau.generic_period_end(p_code,p_config->>'frequency') < (p_config->>'incremental_as_of')::date
  END
$$;

CREATE OR REPLACE VIEW taldau.bronze_snapshot_available_periods AS
SELECT DISTINCT s.snapshot_id,k.code AS period_code
FROM taldau.bronze_snapshots s
JOIN taldau.bronze_run_raw r ON r.run_id=s.discovery_run_id
  OR r.run_id IN (SELECT c.run_id FROM taldau.bronze_chunks c WHERE c.snapshot_id=s.snapshot_id)
CROSS JOIN LATERAL jsonb_array_elements(r.response_data) n(node)
CROSS JOIN LATERAL (
  SELECT regexp_replace(key,'^y','') AS code FROM jsonb_object_keys(n.node) key
  WHERE regexp_replace(key,'^y','') ~ (s.source_config->>'period_code_regex')
) k
WHERE taldau.generic_bigint(right(k.code,4)) BETWEEN s.year_start AND s.year_end
  AND n.node ? k.code AND n.node ? ('y'||k.code)
  AND taldau.generic_period_available(k.code,s.source_config);

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
      AND taldau.generic_period_available(regexp_replace(key,'^y',''),s.source_config)
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

