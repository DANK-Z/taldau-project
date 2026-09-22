-- Verified production source registry for the 2023-2026 multi-indicator ELT.
-- Additive and idempotent: existing snapshots keep their frozen source_config.
CREATE SCHEMA IF NOT EXISTS taldau;

CREATE OR REPLACE FUNCTION taldau.generic_period_start(p_code text,p_frequency text) RETURNS date
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  period_month integer;
BEGIN
  IF p_frequency='monthly' AND p_code ~ '^(0[1-9]|1[0-2])[0-9]{4}$' THEN
    RETURN make_date(right(p_code,4)::int,left(p_code,2)::int,1);
  ELSIF p_frequency='quarterly' AND p_code ~ '^(03|06|09|12)[0-9]{4}$' THEN
    period_month:=left(p_code,2)::int;
    RETURN make_date(right(p_code,4)::int,period_month-2,1);
  ELSIF p_frequency='quarterly' AND p_code ~ '^Q[1-4][0-9]{4}$' THEN
    RETURN make_date(right(p_code,4)::int,((substring(p_code,2,1)::int-1)*3)+1,1);
  ELSIF p_frequency='annual' AND p_code ~ '^12[0-9]{4}$' THEN
    RETURN make_date(right(p_code,4)::int,1,1);
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

INSERT INTO taldau.metadata_indicator_registry
  (indicator_key,display_name,pipeline_type,indicator_id,period_id,endpoint,dimensions,roots,
   extraction_config,enabled,year_start,year_end)
VALUES
('investments_fixed_assets','Инвестиции в основной капитал','cube',701827,8,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"kato","dic_id":68,"chunk":true},{"key":"krp","dic_id":90},{"key":"sif","dic_id":459},{"key":"gsvziok","dic_id":4043}]'::jsonb,
 '{"kato":"741880","krp":"741927","sif":"807855","gsvziok":"19202525"}'::jsonb,
 '{"strategy":"tree_cube","measure_id":1,"idx":3,"frequency":"monthly","period_semantics":"cumulative","period_code_regex":"^(0[1-9]|1[0-2])[0-9]{4}$","expected_periods_per_year":12}'::jsonb,
 true,2023,2026),
('population','Население','region_metric',703831,7,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"region","dic_id":67,"chunk":true},{"key":"locality_type","dic_id":749},{"key":"sex","dic_id":576},{"key":"population_group","dic_id":1433}]'::jsonb,
 '{"region":"741880","locality_type":"741917","sex":"741935","population_group":"3699122"}'::jsonb,
 '{"strategy":"tree_cube","measure_id":1,"idx":3,"frequency":"annual","period_semantics":"point_in_time_start_period","period_code_regex":"^12[0-9]{4}$","expected_periods_per_year":1}'::jsonb,
 true,2023,2026),
('average_salary','Среднемесячная заработная плата','region_metric',702972,5,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"region","dic_id":68,"chunk":true},{"key":"economic_activity","dic_id":859},{"key":"economy_sector","dic_id":681}]'::jsonb,
 '{"region":"741880","economic_activity":"741885","economy_sector":"808076"}'::jsonb,
 '{"strategy":"tree_cube","measure_id":1,"idx":0,"frequency":"quarterly","period_semantics":"period","period_code_regex":"^(03|06|09|12)[0-9]{4}$","expected_periods_per_year":4}'::jsonb,
 true,2023,2026),
('grp','Валовой региональный продукт','region_metric',2709379,9,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"region","dic_id":67,"chunk":true}]'::jsonb,
 '{"region":"741880"}'::jsonb,
 '{"strategy":"tree_cube","measure_id":1,"idx":0,"frequency":"quarterly","period_semantics":"cumulative","period_code_regex":"^(03|06|09|12)[0-9]{4}$","expected_periods_per_year":4}'::jsonb,
 true,2023,2026),
('agriculture','Сельское хозяйство','cube',701189,8,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"region","dic_id":67,"chunk":true},{"key":"producer_category","dic_id":488},{"key":"price_type","dic_id":773}]'::jsonb,
 '{"region":"741880","producer_category":"450122","price_type":"734928"}'::jsonb,
 '{"strategy":"tree_cube","measure_id":1,"idx":0,"frequency":"monthly","period_semantics":"cumulative","period_code_regex":"^(0[1-9]|1[0-2])[0-9]{4}$","expected_periods_per_year":12}'::jsonb,
 true,2023,2026),
('industry','Промышленность','cube',701592,8,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"region","dic_id":68,"chunk":true},{"key":"enterprise_size","dic_id":90},{"key":"economic_activity","dic_id":4303}]'::jsonb,
 '{"region":"741880","enterprise_size":"741927","economic_activity":"3079117"}'::jsonb,
 '{"strategy":"tree_cube","measure_id":1,"idx":1,"frequency":"monthly","period_semantics":"cumulative","period_code_regex":"^(0[1-9]|1[0-2])[0-9]{4}$","expected_periods_per_year":12}'::jsonb,
 true,2023,2026),
('trade','Торговля','cube',2709782,4,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"region","dic_id":67,"chunk":true},{"key":"period_relation","dic_id":848}]'::jsonb,
 '{"region":"741880","period_relation":"2695732"}'::jsonb,
 '{"strategy":"tree_cube","measure_id":1,"idx":0,"frequency":"monthly","period_semantics":"period","period_code_regex":"^(0[1-9]|1[0-2])[0-9]{4}$","expected_periods_per_year":12}'::jsonb,
 true,2023,2026),
('construction','Строительство','cube',701885,8,
 'https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData',
 '[{"key":"region","dic_id":68,"chunk":true},{"key":"enterprise_size","dic_id":90},{"key":"construction_work_type","dic_id":71}]'::jsonb,
 '{"region":"741880","enterprise_size":"741927","construction_work_type":"741919"}'::jsonb,
 '{"strategy":"tree_cube","measure_id":1,"idx":0,"frequency":"monthly","period_semantics":"cumulative","period_code_regex":"^(0[1-9]|1[0-2])[0-9]{4}$","expected_periods_per_year":12}'::jsonb,
 true,2023,2026)
ON CONFLICT(indicator_key) DO UPDATE SET
  display_name=excluded.display_name,
  pipeline_type=excluded.pipeline_type,
  indicator_id=excluded.indicator_id,
  period_id=excluded.period_id,
  endpoint=excluded.endpoint,
  dimensions=excluded.dimensions,
  roots=excluded.roots,
  extraction_config=excluded.extraction_config,
  enabled=excluded.enabled,
  year_start=excluded.year_start,
  year_end=excluded.year_end,
  updated_at=now();

-- The enabled-indicator view preserves period_semantics because extraction_config
-- is the left operand of its JSONB concatenation.
INSERT INTO taldau.metadata_elt_pipelines(pipeline_id,pipeline_type,config)
SELECT 'statistics_'||indicator_key,pipeline_type,source_config
FROM taldau.metadata_enabled_indicators
ON CONFLICT(pipeline_id) DO UPDATE SET
  pipeline_type=excluded.pipeline_type,
  config=excluded.config;

COMMENT ON FUNCTION taldau.generic_period_start(text,text) IS
 'Converts real Taldau MMYYYY period codes (plus legacy QnYYYY/YYYY) to period starts.';
