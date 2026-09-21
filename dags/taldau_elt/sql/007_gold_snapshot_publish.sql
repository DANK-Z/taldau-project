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
        AND (v.indicator_id<>(s.config->>'indicator_id')::bigint OR extract(year FROM v.period_date)<>s.year
          OR extract(year FROM v.start_date)<>s.year OR extract(year FROM v.end_date)<>s.year)) THEN
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

    INSERT INTO taldau.gold_dim_inv_period
        (indicator_id,reporting_period,period_code,start_date,end_date,period_type)
    SELECT DISTINCT indicator_id,reporting_period,period_code,start_date,end_date,'month_cumulative'
    FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=p_snapshot
    ON CONFLICT(indicator_id,reporting_period) DO UPDATE SET
        period_code=EXCLUDED.period_code,start_date=EXCLUDED.start_date,end_date=EXCLUDED.end_date;

    DELETE FROM taldau.gold_fact_inv_fixed_assets f USING taldau.gold_dim_inv_period p
    WHERE p.indicator_id=f.indicator_id AND p.reporting_period=f.reporting_period
        AND f.indicator_id=(s.config->>'indicator_id')::bigint AND extract(year FROM p.start_date)=s.year;
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
            AND extract(year FROM start_date)=s.year)
        UNION ALL
        (SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
         FROM taldau.gold_v_inv_fixed_assets WHERE indicator_id=(s.config->>'indicator_id')::bigint
            AND extract(year FROM start_date)=s.year
         EXCEPT
         SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,value
         FROM taldau.silver_inv_fixed_assets WHERE source_snapshot_id=p_snapshot)
    ) THEN RAISE EXCEPTION 'Gold/Silver snapshot mismatch'; END IF;
    RETURN inserted;
END $$;
