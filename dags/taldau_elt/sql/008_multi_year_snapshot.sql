-- Additive range/reuse support; no extraction, staging or publication is executed.
ALTER TABLE taldau.bronze_inv_snapshots ADD COLUMN IF NOT EXISTS year_start integer;
ALTER TABLE taldau.bronze_inv_snapshots ADD COLUMN IF NOT EXISTS year_end integer;
ALTER TABLE taldau.bronze_inv_snapshots ADD COLUMN IF NOT EXISTS reuse_snapshot_id text REFERENCES taldau.bronze_inv_snapshots(snapshot_id);
ALTER TABLE taldau.bronze_inv_snapshots DROP CONSTRAINT IF EXISTS bronze_inv_snapshots_year_check;
DO $$ BEGIN
    IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='taldau.bronze_inv_snapshots'::regclass AND conname='bronze_inv_snapshot_range_check') THEN
        ALTER TABLE taldau.bronze_inv_snapshots ADD CONSTRAINT bronze_inv_snapshot_range_check CHECK (
            (year_start IS NULL AND year_end IS NULL AND year BETWEEN 2023 AND 2026)
            OR (year_start IS NOT NULL AND year_end IS NOT NULL AND year=year_start
                AND year_start BETWEEN 2023 AND 2026 AND year_end BETWEEN year_start AND 2026));
    END IF;
END $$;
-- Legacy year stays intact; existing rows need no UPDATE/backfill.
CREATE TABLE IF NOT EXISTS taldau.bronze_inv_reuse_raw (
    snapshot_id text NOT NULL REFERENCES taldau.bronze_inv_snapshots,
    request_hash text NOT NULL,
    raw_id bigint NOT NULL REFERENCES taldau.bronze_taldau_api_raw,
    PRIMARY KEY(snapshot_id,request_hash)
);
CREATE INDEX IF NOT EXISTS bronze_inv_reuse_raw_id_idx ON taldau.bronze_inv_reuse_raw(raw_id);

-- Logical run ownership is separate from the immutable physical raw.run_id.
-- Include owned raw without checkpoints too, so coverage still detects untracked responses.
CREATE OR REPLACE VIEW taldau.bronze_inv_run_raw AS
SELECT r.id,r.run_id,r.indicator_id,r.endpoint,r.period_id,r.request_params,r.request_hash,
       r.response_data,r.response_text,r.response_hash,r.http_status,r.dimension,r.tree_depth,r.loaded_at,
       r.run_id AS source_run_id
FROM taldau.bronze_taldau_api_raw r
UNION ALL
SELECT r.id,q.run_id,r.indicator_id,r.endpoint,r.period_id,r.request_params,r.request_hash,
       r.response_data,r.response_text,r.response_hash,r.http_status,r.dimension,r.tree_depth,r.loaded_at,
       r.run_id AS source_run_id
FROM taldau.bronze_inv_request_tasks q JOIN taldau.bronze_taldau_api_raw r ON r.id=q.raw_id
WHERE q.state='complete' AND r.run_id<>q.run_id;

CREATE OR REPLACE VIEW taldau.quality_inv_year_coverage AS
SELECT s.snapshot_id,y.year,coalesce(a.months_present,0) AS months_present,
       coalesce(a.period_codes,'{}'::text[]) AS period_codes,a.available_through,
       coalesce(a.numeric_rows,0) AS numeric_rows,coalesce(a.x_rows,0) AS x_rows,
       CASE WHEN coalesce(a.months_present,0)<12 AND y.year<extract(year FROM current_date)
            THEN 'historical_year_has_fewer_than_12_months'
            WHEN coalesce(a.months_present,0)<12 THEN 'partial_year' END AS warning
FROM taldau.bronze_inv_snapshots s
CROSS JOIN LATERAL generate_series(coalesce(s.year_start,s.year),coalesce(s.year_end,s.year)) y(year)
LEFT JOIN LATERAL (
    SELECT count(DISTINCT period_code) AS months_present,array_agg(DISTINCT period_code ORDER BY period_code) AS period_codes,
           max(period_code) AS available_through,count(value) AS numeric_rows,
           count(*) FILTER(WHERE raw_value='x') AS x_rows
    FROM taldau.staging_inv_year_cells v WHERE v.snapshot_id=s.snapshot_id
      AND v.has_value AND v.has_period AND v.reporting_period IS NOT NULL
      AND v.period_code ~ '^(0[1-9]|1[0-2])[0-9]{4}$' AND right(v.period_code,4)=y.year::text
) a ON true;

CREATE OR REPLACE FUNCTION taldau.bronze_stage_inv_run(p_snapshot text,p_run text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM taldau.bronze_inv_snapshot_members WHERE snapshot_id=p_snapshot AND run_id=p_run;
    INSERT INTO taldau.bronze_inv_snapshot_members
    SELECT p_snapshot,r.run_id,r.id,n.ord,r.dimension,taldau.bronze_inv_int(n.node->>'id'),
        btrim(n.node->>'text'),taldau.bronze_inv_int(r.request_params->>'p_parent_id'),r.tree_depth,
        CASE lower(n.node->>'leaf') WHEN 'true' THEN true WHEN 'false' THEN false END,
        r.request_params->>'p_terms'
    FROM taldau.bronze_inv_run_raw r
    CROSS JOIN LATERAL jsonb_array_elements(r.response_data) WITH ORDINALITY n(node,ord)
    WHERE r.run_id=p_run;
END $$;

CREATE OR REPLACE FUNCTION taldau.staging_stage_inv_chunk(p_chunk bigint) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE c taldau.bronze_inv_chunks; s taldau.bronze_inv_snapshots; inserted bigint;
BEGIN
    SELECT * INTO STRICT c FROM taldau.bronze_inv_chunks WHERE chunk_id=p_chunk FOR UPDATE;
    SELECT * INTO STRICT s FROM taldau.bronze_inv_snapshots WHERE snapshot_id=c.snapshot_id;
    IF c.state<>'running' OR s.state<>'loading' THEN RAISE EXCEPTION 'Chunk is not running'; END IF;
    IF (SELECT status FROM taldau.bronze_extraction_runs WHERE run_id=c.run_id)<>'bronze_complete' THEN
        RAISE EXCEPTION 'Traversal is not finished';
    END IF;
    PERFORM taldau.bronze_stage_inv_run(c.snapshot_id,c.run_id);
    DELETE FROM taldau.staging_inv_year_cells WHERE chunk_id=p_chunk;
    INSERT INTO taldau.staging_inv_year_cells
    SELECT c.snapshot_id,c.chunk_id,r.run_id,r.id,n.ord,r.indicator_id,
        taldau.bronze_inv_int(split_part(r.request_params->>'p_terms',',',1)),
        taldau.bronze_inv_int(split_part(r.request_params->>'p_terms',',',2)),
        taldau.bronze_inv_int(split_part(r.request_params->>'p_terms',',',3)),
        taldau.bronze_inv_int(n.node->>'id'),k.code,
        taldau.bronze_inv_int(n.node->>k.code),n.node->>k.code,
        n.node ? ('y'||k.code),n.node ? k.code,n.node->>('y'||k.code),
        CASE WHEN n.node->>('y'||k.code) ~ '^[+-]?[0-9]+([.][0-9]+)?$'
             THEN (n.node->>('y'||k.code))::numeric END,n.node->>'measureName'
    FROM taldau.bronze_inv_run_raw r
    CROSS JOIN LATERAL jsonb_array_elements(r.response_data) WITH ORDINALITY n(node,ord)
    CROSS JOIN LATERAL (
        -- Union of observed value/period keys also exposes orphan pairs to validation.
        SELECT DISTINCT regexp_replace(key,'^y','') AS code FROM jsonb_object_keys(n.node) key
        WHERE key ~ '^y?[0-9]{6}$' AND taldau.bronze_inv_int(right(key,4)) BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year)
    ) k
    WHERE r.run_id=c.run_id AND r.dimension='gsvziok';
    GET DIAGNOSTICS inserted=ROW_COUNT;
    UPDATE taldau.bronze_inv_chunks SET staged_at=now() WHERE chunk_id=p_chunk;
    RETURN inserted;
END $$;

CREATE OR REPLACE FUNCTION taldau.quality_inv_run_coverage(p_run text) RETURNS bigint
LANGUAGE sql STABLE AS $$
WITH cfg AS (SELECT config,scope FROM taldau.bronze_extraction_runs WHERE run_id=p_run),
r AS MATERIALIZED (SELECT * FROM taldau.bronze_inv_run_raw WHERE run_id=p_run),
n AS MATERIALIZED (SELECT r.dimension,r.request_params,node FROM r CROSS JOIN LATERAL jsonb_array_elements(response_data) node),
errors AS (
 SELECT 1 FROM taldau.bronze_inv_request_tasks q LEFT JOIN r ON r.id=q.raw_id
 WHERE q.run_id=p_run AND (q.state<>'complete' OR r.id IS NULL OR r.run_id<>q.run_id
       OR r.request_hash<>q.request_hash OR r.request_params<>q.request_params OR r.dimension<>q.dimension
       OR (r.source_run_id<>q.run_id AND NOT EXISTS(SELECT 1 FROM taldau.bronze_inv_reuse_raw reuse
           WHERE reuse.snapshot_id=(SELECT scope->>'snapshot_id' FROM taldau.bronze_extraction_runs WHERE run_id=p_run)
           AND reuse.raw_id=r.id AND reuse.request_hash=r.request_hash)))
 UNION ALL
 SELECT 1 FROM r WHERE NOT EXISTS (SELECT 1 FROM taldau.bronze_inv_request_tasks q
     WHERE q.run_id=p_run AND q.raw_id=r.id AND q.state='complete')
 UNION ALL
 SELECT 1 FROM n WHERE taldau.bronze_inv_int(node->>'id') IS NULL OR coalesce(btrim(node->>'text'),'')=''
     OR lower(coalesce(node->>'leaf','')) NOT IN ('true','false')
 UNION ALL
 SELECT 1 FROM r,cfg WHERE r.endpoint IS DISTINCT FROM (config->>'endpoint')
     OR r.indicator_id IS DISTINCT FROM (config->>'indicator_id')::bigint
     OR r.period_id IS DISTINCT FROM (config->>'period_id')::int
     OR request_params->>'p_index_id' IS DISTINCT FROM config->>'indicator_id'
     OR request_params->>'p_period_id' IS DISTINCT FROM config->>'period_id'
     OR request_params->>'p_measure_id' IS DISTINCT FROM config->>'measure_id'
     OR request_params->>'p_dicIds' IS DISTINCT FROM config->>'dic_ids'
     OR request_params->>'idx' IS DISTINCT FROM config->>'idx'
     OR request_params->>'p_term_id' IS DISTINCT FROM config->'roots'->>r.dimension
     OR (scope->>'kind'='territory' AND
          (r.dimension='kato' OR split_part(request_params->>'p_terms',',',1) IS DISTINCT FROM scope->>'kato_id'))
     OR (scope->>'kind'='discovery' AND r.dimension<>'kato')
 UNION ALL
 SELECT 1 FROM cfg WHERE NOT EXISTS (SELECT 1 FROM r
     WHERE dimension=CASE WHEN scope->>'kind'='discovery' THEN 'kato' ELSE 'krp' END
       AND request_params->>'p_parent_id'='')
 UNION ALL
 -- Every non-leaf must have its exact children request, including empty responses.
 SELECT 1 FROM n WHERE lower(node->>'leaf')='false' AND NOT EXISTS (
     SELECT 1 FROM r child WHERE child.dimension=n.dimension
       AND child.request_params->>'p_parent_id'=n.node->>'id'
       AND (child.request_params-'p_parent_id')=(n.request_params-'p_parent_id'))
 UNION ALL
 -- Every observed KRP member creates a SIF root traversal, even when the parent bears values.
 SELECT 1 FROM n,cfg WHERE n.dimension='krp' AND NOT EXISTS (
     SELECT 1 FROM r child WHERE child.dimension='sif' AND child.request_params->>'p_parent_id'=''
       AND child.request_params->>'p_terms'=concat_ws(',',split_part(n.request_params->>'p_terms',',',1),
           n.node->>'id',config->'roots'->>'sif',config->'roots'->>'gsvziok'))
 UNION ALL
 SELECT 1 FROM n,cfg WHERE n.dimension='sif' AND NOT EXISTS (
     SELECT 1 FROM r child WHERE child.dimension='gsvziok' AND child.request_params->>'p_parent_id'=''
       AND child.request_params->>'p_terms'=concat_ws(',',split_part(n.request_params->>'p_terms',',',1),
           split_part(n.request_params->>'p_terms',',',2),n.node->>'id',config->'roots'->>'gsvziok'))
)
SELECT count(*) FROM errors
$$;

CREATE OR REPLACE FUNCTION taldau.quality_inv_discovery_errors(p_snapshot text) RETURNS bigint
LANGUAGE sql STABLE AS $$
WITH s AS (SELECT * FROM taldau.bronze_inv_snapshots WHERE snapshot_id=p_snapshot),
m AS MATERIALIZED (SELECT m.* FROM taldau.bronze_inv_snapshot_members m,s
                    WHERE m.run_id=s.discovery_run_id),
errors AS (
 SELECT 1 FROM s WHERE NOT EXISTS (SELECT 1 FROM m WHERE member_id=(s.config->'roots'->>'kato')::bigint AND parent_id IS NULL)
 UNION ALL
 SELECT 1 FROM m WHERE member_id IS NULL OR member_name IS NULL OR member_name='' OR dimension<>'kato' OR leaf IS NULL
 UNION ALL
 SELECT 1 FROM m GROUP BY member_id HAVING count(*)>1
 UNION ALL
 SELECT 1 FROM m c,s WHERE (c.parent_id IS NULL AND c.member_id<>(s.config->'roots'->>'kato')::bigint)
     OR (c.parent_id IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM m p WHERE p.member_id=c.parent_id AND p.tree_depth+1=c.tree_depth AND NOT p.leaf))
)
SELECT count(*) FROM errors
$$;

CREATE OR REPLACE FUNCTION taldau.quality_validate_inv_snapshot(p_snapshot text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE s taldau.bronze_inv_snapshots; bad bigint; cells bigint;
BEGIN
    SELECT * INTO STRICT s FROM taldau.bronze_inv_snapshots WHERE snapshot_id=p_snapshot FOR UPDATE;
    DELETE FROM taldau.quality_inv_snapshot_checks WHERE snapshot_id=p_snapshot;
    INSERT INTO taldau.quality_inv_snapshot_checks(snapshot_id,check_name,violations)
    SELECT p_snapshot,'discovery_complete',CASE WHEN s.discovery_complete AND s.expected_chunks>0
        AND (SELECT status FROM taldau.bronze_extraction_runs WHERE run_id=s.discovery_run_id)='bronze_complete'
        THEN 0 ELSE 1 END
    UNION ALL SELECT p_snapshot,'territory_hierarchy',taldau.quality_inv_discovery_errors(p_snapshot)
    UNION ALL SELECT p_snapshot,'discovery_request_coverage',taldau.quality_inv_run_coverage(s.discovery_run_id)
    UNION ALL SELECT p_snapshot,'chunk_inventory',count(*) FROM (
        (SELECT member_id FROM taldau.bronze_inv_snapshot_members WHERE run_id=s.discovery_run_id
         EXCEPT SELECT territory_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=p_snapshot)
        UNION ALL
        (SELECT territory_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=p_snapshot
         EXCEPT SELECT member_id FROM taldau.bronze_inv_snapshot_members WHERE run_id=s.discovery_run_id)
    ) d
    UNION ALL SELECT p_snapshot,'expected_chunk_count',CASE WHEN
        (SELECT count(*) FROM taldau.bronze_inv_chunks WHERE snapshot_id=p_snapshot)=s.expected_chunks THEN 0 ELSE 1 END
    UNION ALL SELECT p_snapshot,'incomplete_chunks',count(*) FROM taldau.bronze_inv_chunks c
        JOIN taldau.bronze_extraction_runs r USING(run_id) WHERE c.snapshot_id=p_snapshot
        AND (c.state<>'complete' OR c.staged_at IS NULL OR c.completed_at IS NULL
          OR r.status<>'bronze_complete' OR c.raw_count IS DISTINCT FROM
             (SELECT count(*) FROM taldau.bronze_inv_run_raw raw WHERE raw.run_id=c.run_id))
    UNION ALL SELECT p_snapshot,'chunk_request_coverage',coalesce(sum(taldau.quality_inv_run_coverage(c.run_id)),0)
        FROM taldau.bronze_inv_chunks c WHERE c.snapshot_id=p_snapshot
    UNION ALL SELECT p_snapshot,'null_keys',count(*) FROM taldau.staging_inv_year_cells
        WHERE snapshot_id=p_snapshot AND (indicator_id IS NULL OR kato_id IS NULL OR krp_id IS NULL
          OR sif_id IS NULL OR gsvziok_id IS NULL OR reporting_period IS NULL)
    UNION ALL SELECT p_snapshot,'orphan_period_value_pairs',count(*) FROM taldau.staging_inv_year_cells
        WHERE snapshot_id=p_snapshot AND (NOT has_value OR NOT has_period)
    UNION ALL SELECT p_snapshot,'unknown_non_numeric',count(*) FROM taldau.staging_inv_year_cells
        WHERE snapshot_id=p_snapshot AND value IS NULL AND raw_value IS DISTINCT FROM 'x'
    UNION ALL SELECT p_snapshot,'invalid_period_codes',count(*) FROM taldau.staging_inv_year_cells
        WHERE snapshot_id=p_snapshot AND (period_code !~ '^(0[1-9]|1[0-2])[0-9]{4}$' OR taldau.bronze_inv_int(right(period_code,4)) NOT BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year))
    UNION ALL SELECT p_snapshot,'wrong_scope',count(*) FROM taldau.staging_inv_year_cells v
        JOIN taldau.bronze_inv_chunks c USING(chunk_id) WHERE v.snapshot_id=p_snapshot
        AND (v.kato_id IS DISTINCT FROM c.territory_id OR v.indicator_id IS DISTINCT FROM (s.config->>'indicator_id')::bigint
          OR v.run_id<>c.run_id OR v.snapshot_id<>c.snapshot_id)
    UNION ALL SELECT p_snapshot,'duplicate_natural_keys',count(*) FROM (
        SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id
        FROM taldau.staging_inv_year_cells WHERE snapshot_id=p_snapshot
        GROUP BY 1,2,3,4,5,6 HAVING count(*)>1
    ) d
    UNION ALL SELECT p_snapshot,'conflicting_period_mapping',count(*) FROM (
        SELECT period_code FROM taldau.staging_inv_year_cells WHERE snapshot_id=p_snapshot
        GROUP BY 1 HAVING count(DISTINCT reporting_period)>1
        UNION ALL SELECT reporting_period::text FROM taldau.staging_inv_year_cells WHERE snapshot_id=p_snapshot
        GROUP BY 1 HAVING count(DISTINCT period_code)>1
    ) d
    UNION ALL SELECT p_snapshot,'conflicting_hierarchy',count(*) FROM (
        SELECT dimension,member_id FROM taldau.bronze_inv_snapshot_members WHERE snapshot_id=p_snapshot
        GROUP BY 1,2 HAVING count(DISTINCT (member_name,parent_id,tree_depth))>1
    ) d
    UNION ALL SELECT p_snapshot,'invalid_members',count(*) FROM taldau.bronze_inv_snapshot_members
        WHERE snapshot_id=p_snapshot AND (member_id IS NULL OR coalesce(member_name,'')='' OR leaf IS NULL)
    UNION ALL SELECT p_snapshot,'missing_dimension_members',count(*) FROM taldau.staging_inv_year_cells v
        WHERE v.snapshot_id=p_snapshot AND (
            NOT EXISTS(SELECT 1 FROM taldau.bronze_inv_snapshot_members m WHERE m.run_id=s.discovery_run_id AND m.member_id=v.kato_id)
         OR NOT EXISTS(SELECT 1 FROM taldau.bronze_inv_snapshot_members m WHERE m.run_id=v.run_id AND m.dimension='krp' AND m.member_id=v.krp_id)
         OR NOT EXISTS(SELECT 1 FROM taldau.bronze_inv_snapshot_members m WHERE m.run_id=v.run_id AND m.dimension='sif' AND m.member_id=v.sif_id)
         OR NOT EXISTS(SELECT 1 FROM taldau.bronze_inv_snapshot_members m WHERE m.run_id=v.run_id AND m.dimension='gsvziok' AND m.member_id=v.gsvziok_id));

    -- Store actual rows for every observed period, plus zero rows for missing expected months.
    -- Expected counts and period IDs are diagnostics, never a publication filter.
    DELETE FROM taldau.quality_inv_month_diagnostics WHERE snapshot_id=p_snapshot;
    INSERT INTO taldau.quality_inv_month_diagnostics
        (snapshot_id,period_code,reporting_period,numeric_rows,x_rows,invalid_rows,expected_rows,delta)
    WITH actual AS (
        SELECT period_code,coalesce(reporting_period,-1) AS reporting_period,count(value) AS numeric_rows,
            count(*) FILTER(WHERE raw_value='x') AS x_rows,
            count(*) FILTER(WHERE value IS NULL AND raw_value IS DISTINCT FROM 'x') AS invalid_rows
        FROM taldau.staging_inv_year_cells WHERE snapshot_id=p_snapshot GROUP BY 1,2
    )
    SELECT p_snapshot,coalesce(a.period_code,e.period_code),coalesce(a.reporting_period,e.reporting_period),
        coalesce(a.numeric_rows,0),coalesce(a.x_rows,0),coalesce(a.invalid_rows,0),e.expected_rows,
        coalesce(a.numeric_rows,0)-e.expected_rows
    FROM actual a FULL JOIN (SELECT * FROM taldau.quality_inv_month_expectations WHERE year BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year)) e
        ON e.period_code=a.period_code AND e.reporting_period=a.reporting_period;
    SELECT coalesce(sum(violations),0) INTO bad FROM taldau.quality_inv_snapshot_checks WHERE snapshot_id=p_snapshot;
    SELECT count(value) INTO cells FROM taldau.staging_inv_year_cells WHERE snapshot_id=p_snapshot;
    -- A wholly empty response set should be reviewed rather than erase the current year.
    INSERT INTO taldau.quality_inv_snapshot_checks VALUES(p_snapshot,'empty_snapshot',CASE WHEN cells=0 THEN 1 ELSE 0 END,now());
    IF cells=0 THEN bad:=bad+1; END IF;
    UPDATE taldau.bronze_inv_snapshots SET state=CASE WHEN bad>0 THEN 'failed' WHEN state='published' THEN state ELSE 'validated' END,
        validated_at=CASE WHEN bad=0 THEN now() ELSE NULL END,
        last_error=CASE WHEN bad>0 THEN 'SQL validation failed: see taldau.quality_inv_snapshot_checks' ELSE NULL END
    WHERE snapshot_id=p_snapshot;
    RETURN jsonb_build_object('snapshot_id',p_snapshot,'valid',bad=0,'violations',bad,'numeric_rows',cells,
        'ets_expected',(SELECT sum(expected_rows) FROM taldau.quality_inv_month_expectations WHERE year BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year)),
        'delta',(SELECT sum(delta) FROM taldau.quality_inv_month_diagnostics WHERE snapshot_id=p_snapshot));
END $$;

CREATE OR REPLACE FUNCTION taldau.silver_publish_inv_snapshot(p_snapshot text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE s taldau.bronze_inv_snapshots; check_result jsonb; inserted bigint;
BEGIN
    -- Serializes with the pilot publisher as well as other full snapshots.
    PERFORM pg_advisory_xact_lock(hashtextextended('taldau.silver_inv_fixed_assets:pilot',0));
    LOCK TABLE taldau.bronze_inv_chunks IN SHARE MODE;
    LOCK TABLE taldau.silver_inv_fixed_assets IN SHARE ROW EXCLUSIVE MODE;
    SELECT * INTO STRICT s FROM taldau.bronze_inv_snapshots WHERE snapshot_id=p_snapshot FOR UPDATE;
    check_result:=taldau.quality_validate_inv_snapshot(p_snapshot);
    IF NOT (check_result->>'valid')::boolean THEN RAISE EXCEPTION 'Snapshot is incomplete/invalid: %',check_result; END IF;
    IF EXISTS(SELECT 1 FROM taldau.silver_inv_fixed_assets v JOIN taldau.bronze_extraction_runs r ON r.run_id=v.source_run_id
        WHERE v.indicator_id=(s.config->>'indicator_id')::bigint AND extract(year FROM v.period_date) BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year)
        AND (r.started_at>s.created_at OR (
            v.source_raw_id NOT IN (SELECT raw.id FROM taldau.bronze_inv_run_raw raw
                WHERE raw.run_id=s.discovery_run_id OR raw.run_id IN (SELECT run_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=p_snapshot))
            AND r.started_at>(SELECT min(raw.loaded_at) FROM taldau.bronze_inv_run_raw raw
                WHERE raw.run_id=s.discovery_run_id OR raw.run_id IN (SELECT run_id FROM taldau.bronze_inv_chunks WHERE snapshot_id=p_snapshot)))) AND (v.source_snapshot_id IS DISTINCT FROM p_snapshot)) THEN
        RAISE EXCEPTION 'Newer data is already published for this year; old snapshot cannot overwrite it';
    END IF;
    INSERT INTO taldau.silver_dim_territory
        (territory_id,territory_name,parent_id,source_tree_depth,source_raw_id,source_run_id)
    SELECT member_id,member_name,parent_id,tree_depth,raw_id,run_id
    FROM taldau.bronze_inv_snapshot_members WHERE run_id=s.discovery_run_id
    ON CONFLICT(territory_id) DO UPDATE SET territory_name=EXCLUDED.territory_name,
        parent_id=EXCLUDED.parent_id,source_tree_depth=EXCLUDED.source_tree_depth,
        source_raw_id=EXCLUDED.source_raw_id,source_run_id=EXCLUDED.source_run_id,loaded_at=now();
    -- All-or-nothing replacement of this indicator/year, including disappeared or now-x cells.
    DELETE FROM taldau.silver_inv_fixed_assets WHERE indicator_id=(s.config->>'indicator_id')::bigint
        AND extract(year FROM period_date) BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year);
    INSERT INTO taldau.silver_inv_fixed_assets
        (indicator_id,kato_id,kato_name,krp_id,krp_name,sif_id,sif_name,gsvziok_id,gsvziok_name,
         period_code,reporting_period,period_date,start_date,end_date,value,value_measure,
         source_run_id,source_raw_id,source_node_ordinal,source_snapshot_id)
    WITH members AS MATERIALIZED (
        SELECT DISTINCT dimension,member_id,member_name FROM taldau.bronze_inv_snapshot_members WHERE snapshot_id=p_snapshot
    )
    SELECT v.indicator_id,v.kato_id,k.member_name,v.krp_id,r.member_name,v.sif_id,f.member_name,
        v.gsvziok_id,g.member_name,v.period_code,v.reporting_period,
        (d.month_start+interval '1 month - 1 day')::date,make_date(right(v.period_code,4)::int,1,1),
        (d.month_start+interval '1 month - 1 day')::date,v.value,v.value_measure,
        v.run_id,v.raw_id,v.node_ordinal,p_snapshot
    FROM taldau.staging_inv_year_cells v
    JOIN members k ON k.dimension='kato' AND k.member_id=v.kato_id
    JOIN members r ON r.dimension='krp' AND r.member_id=v.krp_id
    JOIN members f ON f.dimension='sif' AND f.member_id=v.sif_id
    JOIN members g ON g.dimension='gsvziok' AND g.member_id=v.gsvziok_id
    CROSS JOIN LATERAL (SELECT make_date(right(v.period_code,4)::int,left(v.period_code,2)::int,1) AS month_start) d
    WHERE v.snapshot_id=p_snapshot AND v.value IS NOT NULL;
    GET DIAGNOSTICS inserted=ROW_COUNT;
    IF inserted<>(check_result->>'numeric_rows')::bigint THEN RAISE EXCEPTION 'Publication lost rows'; END IF;
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
    ) THEN RAISE EXCEPTION 'Published Silver differs from validated staging'; END IF;
    UPDATE taldau.bronze_inv_snapshots SET state='published',published_at=now() WHERE snapshot_id=p_snapshot;
    RETURN inserted;
END $$;

-- Manual publication only. Called after Silver in the same application transaction.
CREATE OR REPLACE FUNCTION taldau.gold_publish_inv_snapshot(p_snapshot text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE s taldau.bronze_inv_snapshots; check_result jsonb; silver_rows bigint; inserted bigint;
BEGIN
    -- Use the same lock and ordering as the pilot/full Silver publishers.
    PERFORM pg_advisory_xact_lock(hashtextextended('taldau.silver_inv_fixed_assets:pilot',0));
    LOCK TABLE taldau.bronze_inv_chunks IN SHARE MODE;
    LOCK TABLE taldau.silver_inv_fixed_assets IN SHARE ROW EXCLUSIVE MODE;
    SELECT * INTO STRICT s FROM taldau.bronze_inv_snapshots WHERE snapshot_id=p_snapshot FOR UPDATE;
    IF s.state<>'published' THEN
        RAISE EXCEPTION 'Snapshot must be published in Silver before Gold';
    END IF;
    check_result:=taldau.quality_validate_inv_snapshot(p_snapshot);
    IF NOT (check_result->>'valid')::boolean THEN
        RAISE EXCEPTION 'Snapshot is incomplete/invalid: %',check_result;
    END IF;
    SELECT count(*) INTO silver_rows FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=p_snapshot;
    IF silver_rows<>(check_result->>'numeric_rows')::bigint THEN
        RAISE EXCEPTION 'Silver snapshot count % differs from validated numeric rows %',
            silver_rows,check_result->>'numeric_rows';
    END IF;
    IF EXISTS (SELECT 1 FROM taldau.silver_inv_fixed_assets v WHERE v.source_snapshot_id=p_snapshot
        AND (v.indicator_id<>(s.config->>'indicator_id')::bigint OR extract(year FROM v.period_date) NOT BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year)
          OR extract(year FROM v.start_date) NOT BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year) OR extract(year FROM v.end_date) NOT BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year))) THEN
        RAISE EXCEPTION 'Silver snapshot contains rows outside its indicator/year';
    END IF;
    -- Do not reuse stale Gold members when the current snapshot lacks a source ID.
    IF EXISTS (
        SELECT 1 FROM taldau.silver_inv_fixed_assets v
        CROSS JOIN LATERAL (VALUES ('kato',v.kato_id),('krp',v.krp_id),
                                   ('sif',v.sif_id),('gsvziok',v.gsvziok_id)) d(dimension,member_id)
        WHERE v.source_snapshot_id=p_snapshot AND NOT EXISTS (
            SELECT 1 FROM taldau.bronze_inv_snapshot_members m WHERE m.snapshot_id=p_snapshot
                AND m.dimension=d.dimension AND m.member_id=d.member_id)
    ) THEN RAISE EXCEPTION 'Missing dimension member in snapshot'; END IF;

    LOCK TABLE taldau.gold_dim_inv_member, taldau.gold_dim_inv_period, taldau.gold_fact_inv_fixed_assets
        IN SHARE ROW EXCLUSIVE MODE;
    INSERT INTO taldau.gold_dim_inv_member
        (indicator_id,dimension,source_id,member_name,parent_source_id,source_tree_depth,is_total,source_run_id)
    SELECT DISTINCT ON(m.dimension,m.member_id)
        (s.config->>'indicator_id')::bigint,m.dimension,m.member_id,m.member_name,m.parent_id,m.tree_depth,
        m.member_id::text=(s.config->'roots'->>m.dimension),m.run_id
    FROM taldau.bronze_inv_snapshot_members m WHERE m.snapshot_id=p_snapshot
        AND m.dimension IN ('kato','krp','sif','gsvziok')
    ORDER BY m.dimension,m.member_id,m.raw_id,m.node_ordinal
    ON CONFLICT(indicator_id,dimension,source_id) DO UPDATE SET
        member_name=EXCLUDED.member_name,parent_source_id=EXCLUDED.parent_source_id,
        source_tree_depth=EXCLUDED.source_tree_depth,is_total=EXCLUDED.is_total,
        source_run_id=EXCLUDED.source_run_id;

    -- A source period ID must not move existing facts across the publication boundary.
    IF EXISTS (
        SELECT 1 FROM taldau.silver_inv_fixed_assets v JOIN taldau.gold_dim_inv_period p
            USING(indicator_id,reporting_period)
        WHERE v.source_snapshot_id=p_snapshot AND
            (p.period_code IS DISTINCT FROM v.period_code OR p.start_date IS DISTINCT FROM v.start_date
             OR p.end_date IS DISTINCT FROM v.end_date)
    ) THEN RAISE EXCEPTION 'Reporting period conflicts with existing Gold dates'; END IF;
    INSERT INTO taldau.gold_dim_inv_period
        (indicator_id,reporting_period,period_code,start_date,end_date,period_type)
    SELECT DISTINCT indicator_id,reporting_period,period_code,start_date,end_date,'month_cumulative'
    FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=p_snapshot
    ON CONFLICT(indicator_id,reporting_period) DO UPDATE SET
        period_code=EXCLUDED.period_code,start_date=EXCLUDED.start_date,end_date=EXCLUDED.end_date;

    DELETE FROM taldau.gold_fact_inv_fixed_assets f USING taldau.gold_dim_inv_period p
    WHERE p.indicator_id=f.indicator_id AND p.reporting_period=f.reporting_period
        AND f.indicator_id=(s.config->>'indicator_id')::bigint AND extract(year FROM p.start_date) BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year);
    INSERT INTO taldau.gold_fact_inv_fixed_assets
        (indicator_id,reporting_period,kato_key,krp_key,sif_key,gsvziok_key,value,value_measure,source_run_id,source_raw_id)
    SELECT v.indicator_id,v.reporting_period,k.member_key,r.member_key,f.member_key,g.member_key,
        v.value,v.value_measure,v.source_run_id,v.source_raw_id
    FROM taldau.silver_inv_fixed_assets v
    JOIN taldau.gold_dim_inv_member k ON k.indicator_id=v.indicator_id AND k.dimension='kato' AND k.source_id=v.kato_id
    JOIN taldau.gold_dim_inv_member r ON r.indicator_id=v.indicator_id AND r.dimension='krp' AND r.source_id=v.krp_id
    JOIN taldau.gold_dim_inv_member f ON f.indicator_id=v.indicator_id AND f.dimension='sif' AND f.source_id=v.sif_id
    JOIN taldau.gold_dim_inv_member g ON g.indicator_id=v.indicator_id AND g.dimension='gsvziok' AND g.source_id=v.gsvziok_id
    WHERE v.source_snapshot_id=p_snapshot;
    GET DIAGNOSTICS inserted=ROW_COUNT;
    IF inserted<>silver_rows THEN
        RAISE EXCEPTION 'Gold row loss: inserted %, Silver %',inserted,silver_rows;
    END IF;
    -- Compare the entire target Gold slice, including any unexpected extra rows.
    IF EXISTS (
        (SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
         FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=p_snapshot
         EXCEPT
         SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
         FROM taldau.gold_v_inv_fixed_assets WHERE indicator_id=(s.config->>'indicator_id')::bigint
            AND extract(year FROM start_date) BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year))
        UNION ALL
        (SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
         FROM taldau.gold_v_inv_fixed_assets WHERE indicator_id=(s.config->>'indicator_id')::bigint
            AND extract(year FROM start_date) BETWEEN coalesce(s.year_start,s.year) AND coalesce(s.year_end,s.year)
         EXCEPT
         SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
         FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=p_snapshot)
    ) THEN RAISE EXCEPTION 'Gold/Silver snapshot mismatch'; END IF;
    RETURN inserted;
END $$;

CREATE OR REPLACE FUNCTION taldau.reconciliation_capture_inv_ets(p_dataset text,p_source regclass,p_year int DEFAULT 2025)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE total bigint;
BEGIN
    IF p_year NOT BETWEEN 2023 AND 2026 THEN RAISE EXCEPTION 'Supported baseline years: 2023..2026'; END IF;
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
        FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=p_snapshot
          AND extract(year FROM period_date)=(SELECT year FROM taldau.reconciliation_ets_inv_datasets WHERE dataset_id=p_dataset) GROUP BY 1,2,3,4,5,6
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
