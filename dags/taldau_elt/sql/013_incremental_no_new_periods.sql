-- Confirmed empty incremental scopes are terminal successes, never publications.
-- Apply after 012, in one transaction with workers stopped. Safe to replay.
ALTER TABLE taldau.bronze_snapshots DROP CONSTRAINT IF EXISTS bronze_snapshots_state_check;
ALTER TABLE taldau.bronze_snapshots ADD CONSTRAINT bronze_snapshots_state_check
  CHECK (state IN ('prepared','discovering','loading','failed','validated','published','no_new_periods'));

-- Diagnostics remain readable even when validation rejects malformed raw.
CREATE OR REPLACE VIEW taldau.bronze_snapshot_available_periods AS
SELECT DISTINCT s.snapshot_id,k.code AS period_code
FROM taldau.bronze_snapshots s
JOIN taldau.bronze_run_raw r ON r.run_id=s.discovery_run_id
  OR r.run_id IN (SELECT c.run_id FROM taldau.bronze_chunks c WHERE c.snapshot_id=s.snapshot_id)
CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(r.response_data)='array'
  THEN r.response_data ELSE '[]'::jsonb END) n(node)
CROSS JOIN LATERAL (
  SELECT regexp_replace(key,'^y','') AS code FROM jsonb_object_keys(CASE WHEN jsonb_typeof(n.node)='object'
    THEN n.node ELSE '{}'::jsonb END) key
  WHERE regexp_replace(key,'^y','') ~ (s.source_config->>'period_code_regex')
) k
WHERE taldau.generic_bigint(right(k.code,4)) BETWEEN s.year_start AND s.year_end
  AND n.node ? k.code AND n.node ? ('y'||k.code)
  AND taldau.generic_period_available(k.code,s.source_config);

-- Replace complete functions rather than patching pg_proc text. Historical validation
-- and scoped publication retain their existing checks and transaction locks.
CREATE OR REPLACE FUNCTION taldau.validate_snapshot(p_snapshot text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE s taldau.bronze_snapshots; bad bigint; numeric_rows bigint; expected int; current_partial boolean; no_new boolean := false;
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

  -- Empty incremental scopes need positive evidence of a successful discovery.
  -- Keep every ordinary validation check; only empty_snapshot can be waived.
  IF s.batch_id IS NOT NULL AND s.source_config ? 'incremental_as_of'
     AND NOT EXISTS (SELECT 1 FROM taldau.staging_observation_cells WHERE snapshot_id=p_snapshot) THEN
    INSERT INTO taldau.quality_snapshot_checks(snapshot_id,check_name,severity,violations,details)
    VALUES
      (p_snapshot,'no_new_periods_discovery_evidence','blocking',CASE WHEN EXISTS (
        SELECT 1 FROM taldau.bronze_extraction_runs run
        JOIN taldau.bronze_run_raw r ON r.run_id=run.run_id
        WHERE run.run_id=s.discovery_run_id AND run.status='bronze_complete'
          AND r.dimension=s.source_config->'dimensions'->0->>'key'
          AND coalesce(r.request_params->>'p_parent_id','')=''
          AND jsonb_array_length(CASE WHEN jsonb_typeof(r.response_data)='array'
              THEN r.response_data ELSE '[]'::jsonb END)>0
      ) THEN 0 ELSE 1 END,'{}'),
      (p_snapshot,'no_new_periods_http_errors','blocking',(
        SELECT count(*) FROM taldau.bronze_run_raw r
        WHERE (r.run_id=s.discovery_run_id OR r.run_id IN (
          SELECT run_id FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot))
          AND (r.http_status IS NULL OR r.http_status NOT BETWEEN 200 AND 299)),'{}'),
      (p_snapshot,'no_new_periods_orphan_keys','blocking',(
        SELECT count(*) FROM taldau.bronze_run_raw r
        CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(r.response_data)='array'
          THEN r.response_data ELSE '[]'::jsonb END) n(node)
        CROSS JOIN LATERAL jsonb_object_keys(CASE WHEN jsonb_typeof(n.node)='object'
          THEN n.node ELSE '{}'::jsonb END) k(key)
        WHERE (r.run_id=s.discovery_run_id OR r.run_id IN (
          SELECT run_id FROM taldau.bronze_chunks WHERE snapshot_id=p_snapshot))
          AND regexp_replace(k.key,'^y','') ~ (s.source_config->>'period_code_regex')
          AND taldau.generic_bigint(right(k.key,4)) BETWEEN s.year_start AND s.year_end
          AND taldau.generic_period_available(regexp_replace(k.key,'^y',''),s.source_config)
          AND NOT (n.node ? regexp_replace(k.key,'^y','')
                   AND n.node ? ('y'||regexp_replace(k.key,'^y','')))),'{}');
    -- Evaluate the periods view only after malformed responses have been rejected.
    IF NOT EXISTS (SELECT 1 FROM taldau.quality_snapshot_checks
        WHERE snapshot_id=p_snapshot AND severity='blocking' AND violations<>0
          AND check_name<>'empty_snapshot') THEN
      no_new := NOT EXISTS (SELECT 1 FROM taldau.bronze_snapshot_available_periods
                           WHERE snapshot_id=p_snapshot);
    END IF;
    IF no_new THEN
      UPDATE taldau.quality_snapshot_checks SET violations=0,
        details=jsonb_build_object('reason','no_new_periods','available_periods',0,'staged_rows',0)
      WHERE snapshot_id=p_snapshot AND check_name='empty_snapshot';
    END IF;
  END IF;

  SELECT coalesce(sum(violations),0) INTO bad FROM taldau.quality_snapshot_checks
  WHERE snapshot_id=p_snapshot AND severity='blocking';
  SELECT count(*) INTO numeric_rows FROM taldau.staging_observation_cells
  WHERE snapshot_id=p_snapshot AND value_status='numeric';
  UPDATE taldau.bronze_snapshots SET state=CASE WHEN no_new THEN 'no_new_periods' WHEN bad=0 THEN 'validated' ELSE 'failed' END,
      validated_at=CASE WHEN bad=0 THEN now() ELSE NULL END,
      last_error=CASE WHEN bad=0 THEN NULL ELSE 'SQL validation failed: see taldau.quality_snapshot_checks' END
  WHERE snapshot_id=p_snapshot;
  RETURN jsonb_build_object('snapshot_id',p_snapshot,'valid',bad=0,'violations',bad,'numeric_rows',numeric_rows,
      'state',CASE WHEN no_new THEN 'no_new_periods' WHEN bad=0 THEN 'validated' ELSE 'failed' END);
END $$;

CREATE OR REPLACE FUNCTION taldau.publish_snapshot(p_snapshot text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE s taldau.bronze_snapshots; result jsonb; inserted bigint; gold_rows bigint; frequency text;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('taldau.generic_publication',0));
  SELECT * INTO STRICT s FROM taldau.bronze_snapshots WHERE snapshot_id=p_snapshot FOR UPDATE;
  IF s.state='no_new_periods' THEN RETURN 0; END IF;
  result:=taldau.validate_snapshot(p_snapshot);
  IF result->>'state'='no_new_periods' THEN RETURN 0; END IF;
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
    SELECT 1 FROM taldau.bronze_snapshots x WHERE x.batch_id=b.batch_id AND x.state NOT IN ('published','no_new_periods'));
  RETURN inserted;
END $$;
