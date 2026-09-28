-- Pre-optimization query: offline JSON parity and EXPLAIN baseline only.
WITH s AS (SELECT * FROM taldau.bronze_snapshots WHERE snapshot_id=%s),
          chunks AS (SELECT c.* FROM taldau.bronze_chunks c JOIN s USING(snapshot_id)),
          cells AS (SELECT v.* FROM taldau.staging_observation_cells v JOIN s USING(snapshot_id))
        SELECT jsonb_build_object(
          'snapshot_id',s.snapshot_id,'indicator_key',s.indicator_key,'state',s.state,
          'year_start',s.year_start,'year_end',s.year_end,
          'no_new_periods',s.state='no_new_periods',
          'staged_rows',(SELECT count(*) FROM cells),
          'available_periods',(SELECT count(*) FROM taldau.bronze_snapshot_available_periods p
             WHERE p.snapshot_id=s.snapshot_id),
          'chunks_total',(SELECT count(*) FROM chunks),
          'chunks_complete',(SELECT count(*) FROM chunks WHERE state='complete'),
          'chunks_failed',(SELECT count(*) FROM chunks WHERE state='failed'),
          'raw_responses',(SELECT count(*) FROM taldau.bronze_run_raw r WHERE r.run_id=s.discovery_run_id
             OR r.run_id IN (SELECT run_id FROM chunks)),
          'numeric_rows',(SELECT count(*) FROM cells WHERE value_status='numeric'),
          'x_rows',(SELECT count(*) FROM cells WHERE value_status='x'),
          'invalid_rows',(SELECT count(*) FROM cells WHERE value_status IN ('invalid','missing')),
          'duplicate_keys',(SELECT count(*) FROM (SELECT reporting_period,coordinates FROM cells
             WHERE value_status='numeric' GROUP BY 1,2 HAVING count(*)>1) d),
          'checks',coalesce((SELECT jsonb_agg(to_jsonb(q) ORDER BY check_name)
             FROM taldau.quality_snapshot_checks q WHERE q.snapshot_id=s.snapshot_id),'[]'::jsonb),
          'periods',coalesce((SELECT jsonb_agg(to_jsonb(p) ORDER BY period_code,reporting_period)
             FROM taldau.quality_period_diagnostics p WHERE p.snapshot_id=s.snapshot_id),'[]'::jsonb)
        ) FROM s
