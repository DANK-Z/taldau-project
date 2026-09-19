CREATE OR REPLACE FUNCTION quality.inv_run_coverage(p_run text) RETURNS bigint
LANGUAGE sql STABLE AS $$
WITH cfg AS (SELECT config,scope FROM bronze.extraction_runs WHERE run_id=p_run),
r AS MATERIALIZED (SELECT * FROM bronze.taldau_api_raw WHERE run_id=p_run),
n AS MATERIALIZED (SELECT r.dimension,r.request_params,node FROM r CROSS JOIN LATERAL jsonb_array_elements(response_data) node),
errors AS (
 SELECT 1 FROM bronze.inv_request_tasks q LEFT JOIN r ON r.id=q.raw_id
 WHERE q.run_id=p_run AND (q.state<>'complete' OR r.id IS NULL OR r.run_id<>q.run_id
       OR r.request_hash<>q.request_hash OR r.request_params<>q.request_params OR r.dimension<>q.dimension)
 UNION ALL
 SELECT 1 FROM r WHERE NOT EXISTS (SELECT 1 FROM bronze.inv_request_tasks q
     WHERE q.run_id=p_run AND q.raw_id=r.id AND q.state='complete')
 UNION ALL
 SELECT 1 FROM n WHERE bronze.inv_int(node->>'id') IS NULL OR coalesce(btrim(node->>'text'),'')=''
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

CREATE OR REPLACE FUNCTION quality.inv_discovery_errors(p_snapshot text) RETURNS bigint
LANGUAGE sql STABLE AS $$
WITH s AS (SELECT * FROM bronze.inv_snapshots WHERE snapshot_id=p_snapshot),
m AS MATERIALIZED (SELECT m.* FROM bronze.inv_snapshot_members m,s
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

CREATE OR REPLACE FUNCTION quality.validate_inv_snapshot(p_snapshot text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE s bronze.inv_snapshots; bad bigint; cells bigint;
BEGIN
    SELECT * INTO STRICT s FROM bronze.inv_snapshots WHERE snapshot_id=p_snapshot FOR UPDATE;
    DELETE FROM quality.inv_snapshot_checks WHERE snapshot_id=p_snapshot;
    INSERT INTO quality.inv_snapshot_checks(snapshot_id,check_name,violations)
    SELECT p_snapshot,'discovery_complete',CASE WHEN s.discovery_complete AND s.expected_chunks>0
        AND (SELECT status FROM bronze.extraction_runs WHERE run_id=s.discovery_run_id)='bronze_complete'
        THEN 0 ELSE 1 END
    UNION ALL SELECT p_snapshot,'territory_hierarchy',quality.inv_discovery_errors(p_snapshot)
    UNION ALL SELECT p_snapshot,'discovery_request_coverage',quality.inv_run_coverage(s.discovery_run_id)
    UNION ALL SELECT p_snapshot,'chunk_inventory',count(*) FROM (
        (SELECT member_id FROM bronze.inv_snapshot_members WHERE run_id=s.discovery_run_id
         EXCEPT SELECT territory_id FROM bronze.inv_chunks WHERE snapshot_id=p_snapshot)
        UNION ALL
        (SELECT territory_id FROM bronze.inv_chunks WHERE snapshot_id=p_snapshot
         EXCEPT SELECT member_id FROM bronze.inv_snapshot_members WHERE run_id=s.discovery_run_id)
    ) d
    UNION ALL SELECT p_snapshot,'expected_chunk_count',CASE WHEN
        (SELECT count(*) FROM bronze.inv_chunks WHERE snapshot_id=p_snapshot)=s.expected_chunks THEN 0 ELSE 1 END
    UNION ALL SELECT p_snapshot,'incomplete_chunks',count(*) FROM bronze.inv_chunks c
        JOIN bronze.extraction_runs r USING(run_id) WHERE c.snapshot_id=p_snapshot
        AND (c.state<>'complete' OR c.staged_at IS NULL OR c.completed_at IS NULL
          OR r.status<>'bronze_complete' OR c.raw_count IS DISTINCT FROM
             (SELECT count(*) FROM bronze.taldau_api_raw raw WHERE raw.run_id=c.run_id))
    UNION ALL SELECT p_snapshot,'chunk_request_coverage',coalesce(sum(quality.inv_run_coverage(c.run_id)),0)
        FROM bronze.inv_chunks c WHERE c.snapshot_id=p_snapshot
    UNION ALL SELECT p_snapshot,'null_keys',count(*) FROM staging.inv_year_cells
        WHERE snapshot_id=p_snapshot AND (indicator_id IS NULL OR kato_id IS NULL OR krp_id IS NULL
          OR sif_id IS NULL OR gsvziok_id IS NULL OR reporting_period IS NULL)
    UNION ALL SELECT p_snapshot,'orphan_period_value_pairs',count(*) FROM staging.inv_year_cells
        WHERE snapshot_id=p_snapshot AND (NOT has_value OR NOT has_period)
    UNION ALL SELECT p_snapshot,'unknown_non_numeric',count(*) FROM staging.inv_year_cells
        WHERE snapshot_id=p_snapshot AND value IS NULL AND raw_value IS DISTINCT FROM 'x'
    UNION ALL SELECT p_snapshot,'invalid_period_codes',count(*) FROM staging.inv_year_cells
        WHERE snapshot_id=p_snapshot AND period_code !~ ('^(0[1-9]|1[0-2])'||s.year::text||'$')
    UNION ALL SELECT p_snapshot,'wrong_scope',count(*) FROM staging.inv_year_cells v
        JOIN bronze.inv_chunks c USING(chunk_id) WHERE v.snapshot_id=p_snapshot
        AND (v.kato_id IS DISTINCT FROM c.territory_id OR v.indicator_id IS DISTINCT FROM (s.config->>'indicator_id')::bigint
          OR v.run_id<>c.run_id OR v.snapshot_id<>c.snapshot_id)
    UNION ALL SELECT p_snapshot,'duplicate_natural_keys',count(*) FROM (
        SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id
        FROM staging.inv_year_cells WHERE snapshot_id=p_snapshot
        GROUP BY 1,2,3,4,5,6 HAVING count(*)>1
    ) d
    UNION ALL SELECT p_snapshot,'conflicting_period_mapping',count(*) FROM (
        SELECT period_code FROM staging.inv_year_cells WHERE snapshot_id=p_snapshot
        GROUP BY 1 HAVING count(DISTINCT reporting_period)>1
        UNION ALL SELECT reporting_period::text FROM staging.inv_year_cells WHERE snapshot_id=p_snapshot
        GROUP BY 1 HAVING count(DISTINCT period_code)>1
    ) d
    UNION ALL SELECT p_snapshot,'conflicting_hierarchy',count(*) FROM (
        SELECT dimension,member_id FROM bronze.inv_snapshot_members WHERE snapshot_id=p_snapshot
        GROUP BY 1,2 HAVING count(DISTINCT (member_name,parent_id,tree_depth))>1
    ) d
    UNION ALL SELECT p_snapshot,'invalid_members',count(*) FROM bronze.inv_snapshot_members
        WHERE snapshot_id=p_snapshot AND (member_id IS NULL OR coalesce(member_name,'')='' OR leaf IS NULL)
    UNION ALL SELECT p_snapshot,'missing_dimension_members',count(*) FROM staging.inv_year_cells v
        WHERE v.snapshot_id=p_snapshot AND (
            NOT EXISTS(SELECT 1 FROM bronze.inv_snapshot_members m WHERE m.run_id=s.discovery_run_id AND m.member_id=v.kato_id)
         OR NOT EXISTS(SELECT 1 FROM bronze.inv_snapshot_members m WHERE m.run_id=v.run_id AND m.dimension='krp' AND m.member_id=v.krp_id)
         OR NOT EXISTS(SELECT 1 FROM bronze.inv_snapshot_members m WHERE m.run_id=v.run_id AND m.dimension='sif' AND m.member_id=v.sif_id));

    -- Store actual rows for every observed period, plus zero rows for missing expected months.
    -- Expected counts and period IDs are diagnostics, never a publication filter.
    DELETE FROM quality.inv_month_diagnostics WHERE snapshot_id=p_snapshot;
    INSERT INTO quality.inv_month_diagnostics
        (snapshot_id,period_code,reporting_period,numeric_rows,x_rows,invalid_rows,expected_rows,delta)
    WITH actual AS (
        SELECT period_code,coalesce(reporting_period,-1) AS reporting_period,count(value) AS numeric_rows,
            count(*) FILTER(WHERE raw_value='x') AS x_rows,
            count(*) FILTER(WHERE value IS NULL AND raw_value IS DISTINCT FROM 'x') AS invalid_rows
        FROM staging.inv_year_cells WHERE snapshot_id=p_snapshot GROUP BY 1,2
    )
    SELECT p_snapshot,coalesce(a.period_code,e.period_code),coalesce(a.reporting_period,e.reporting_period),
        coalesce(a.numeric_rows,0),coalesce(a.x_rows,0),coalesce(a.invalid_rows,0),e.expected_rows,
        coalesce(a.numeric_rows,0)-e.expected_rows
    FROM actual a FULL JOIN (SELECT * FROM quality.inv_month_expectations WHERE year=s.year) e
        ON e.period_code=a.period_code AND e.reporting_period=a.reporting_period;
    SELECT coalesce(sum(violations),0) INTO bad FROM quality.inv_snapshot_checks WHERE snapshot_id=p_snapshot;
    SELECT count(value) INTO cells FROM staging.inv_year_cells WHERE snapshot_id=p_snapshot;
    -- A wholly empty response set should be reviewed rather than erase the current year.
    INSERT INTO quality.inv_snapshot_checks VALUES(p_snapshot,'empty_snapshot',CASE WHEN cells=0 THEN 1 ELSE 0 END,now());
    IF cells=0 THEN bad:=bad+1; END IF;
    UPDATE bronze.inv_snapshots SET state=CASE WHEN bad>0 THEN 'failed' WHEN state='published' THEN state ELSE 'validated' END,
        validated_at=CASE WHEN bad=0 THEN now() ELSE NULL END,
        last_error=CASE WHEN bad>0 THEN 'SQL validation failed: see quality.inv_snapshot_checks' ELSE NULL END
    WHERE snapshot_id=p_snapshot;
    RETURN jsonb_build_object('snapshot_id',p_snapshot,'valid',bad=0,'violations',bad,'numeric_rows',cells,
        'ets_expected',651365,'delta',cells-651365);
END $$;

CREATE OR REPLACE FUNCTION silver.publish_inv_snapshot(p_snapshot text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE s bronze.inv_snapshots; check_result jsonb; inserted bigint;
BEGIN
    -- Serializes with the pilot publisher as well as other full snapshots.
    PERFORM pg_advisory_xact_lock(hashtextextended('silver.inv_fixed_assets:pilot',0));
    LOCK TABLE bronze.inv_chunks IN SHARE MODE;
    LOCK TABLE silver.inv_fixed_assets IN SHARE ROW EXCLUSIVE MODE;
    SELECT * INTO STRICT s FROM bronze.inv_snapshots WHERE snapshot_id=p_snapshot FOR UPDATE;
    check_result:=quality.validate_inv_snapshot(p_snapshot);
    IF NOT (check_result->>'valid')::boolean THEN RAISE EXCEPTION 'Snapshot is incomplete/invalid: %',check_result; END IF;
    IF EXISTS(SELECT 1 FROM silver.inv_fixed_assets v JOIN bronze.extraction_runs r ON r.run_id=v.source_run_id
        WHERE v.indicator_id=(s.config->>'indicator_id')::bigint AND extract(year FROM v.period_date)=s.year
        AND r.started_at>s.created_at AND (v.source_snapshot_id IS DISTINCT FROM p_snapshot)) THEN
        RAISE EXCEPTION 'Newer data is already published for this year; old snapshot cannot overwrite it';
    END IF;
    INSERT INTO silver.dim_territory
        (territory_id,territory_name,parent_id,source_tree_depth,source_raw_id,source_run_id)
    SELECT member_id,member_name,parent_id,tree_depth,raw_id,run_id
    FROM bronze.inv_snapshot_members WHERE run_id=s.discovery_run_id
    ON CONFLICT(territory_id) DO UPDATE SET territory_name=EXCLUDED.territory_name,
        parent_id=EXCLUDED.parent_id,source_tree_depth=EXCLUDED.source_tree_depth,
        source_raw_id=EXCLUDED.source_raw_id,source_run_id=EXCLUDED.source_run_id,loaded_at=now();
    -- All-or-nothing replacement of this indicator/year, including disappeared or now-x cells.
    DELETE FROM silver.inv_fixed_assets WHERE indicator_id=(s.config->>'indicator_id')::bigint
        AND extract(year FROM period_date)=s.year;
    INSERT INTO silver.inv_fixed_assets
        (indicator_id,kato_id,kato_name,krp_id,krp_name,sif_id,sif_name,gsvziok_id,gsvziok_name,
         period_code,reporting_period,period_date,start_date,end_date,value,value_measure,
         source_run_id,source_raw_id,source_node_ordinal,source_snapshot_id)
    WITH members AS MATERIALIZED (
        SELECT DISTINCT dimension,member_id,member_name FROM bronze.inv_snapshot_members WHERE snapshot_id=p_snapshot
    )
    SELECT v.indicator_id,v.kato_id,k.member_name,v.krp_id,r.member_name,v.sif_id,f.member_name,
        v.gsvziok_id,g.member_name,v.period_code,v.reporting_period,
        (d.month_start+interval '1 month - 1 day')::date,make_date(s.year,1,1),
        (d.month_start+interval '1 month - 1 day')::date,v.value,v.value_measure,
        v.run_id,v.raw_id,v.node_ordinal,p_snapshot
    FROM staging.inv_year_cells v
    JOIN members k ON k.dimension='kato' AND k.member_id=v.kato_id
    JOIN members r ON r.dimension='krp' AND r.member_id=v.krp_id
    JOIN members f ON f.dimension='sif' AND f.member_id=v.sif_id
    JOIN members g ON g.dimension='gsvziok' AND g.member_id=v.gsvziok_id
    CROSS JOIN LATERAL (SELECT make_date(s.year,left(v.period_code,2)::int,1) AS month_start) d
    WHERE v.snapshot_id=p_snapshot AND v.value IS NOT NULL;
    GET DIAGNOSTICS inserted=ROW_COUNT;
    IF inserted<>(check_result->>'numeric_rows')::bigint THEN RAISE EXCEPTION 'Publication lost rows'; END IF;
    IF EXISTS (
      (SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value FROM staging.inv_year_cells
       WHERE snapshot_id=p_snapshot AND value IS NOT NULL
       EXCEPT SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value FROM silver.inv_fixed_assets
       WHERE source_snapshot_id=p_snapshot)
      UNION ALL
      (SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value FROM silver.inv_fixed_assets
       WHERE source_snapshot_id=p_snapshot
       EXCEPT SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value FROM staging.inv_year_cells
       WHERE snapshot_id=p_snapshot AND value IS NOT NULL)
    ) THEN RAISE EXCEPTION 'Published Silver differs from validated staging'; END IF;
    UPDATE bronze.inv_snapshots SET state='published',published_at=now() WHERE snapshot_id=p_snapshot;
    RETURN inserted;
END $$;
