-- Traceability mart validation checks
-- Target DB: fund_traceability

USE fund_traceability;

-- 1) Table presence
SELECT
  'required_tables_present' AS check_name,
  CASE WHEN COUNT(*) = 12 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS observed,
  12 AS expected,
  'bridge/fact/agg tables exist' AS detail
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name IN (
    'bridge_thai_master',
    'fact_effective_exposure_stock',
    'fact_effective_exposure_sector',
    'fact_effective_exposure_region',
    'agg_top_holdings',
    'agg_top_holdings_topn',
    'agg_sector_exposure',
    'agg_sector_exposure_topn',
    'agg_country_exposure',
    'agg_country_exposure_topn',
    'agg_region_exposure',
    'agg_dashboard_cards'
  );

-- 2) Non-negative stock metrics
SELECT
  'stock_non_negative_metrics' AS check_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS observed,
  0 AS expected,
  'true_weight_pct >= 0 and true_value_thb >= 0' AS detail
FROM fact_effective_exposure_stock
WHERE COALESCE(true_weight_pct, -1) < 0
   OR COALESCE(true_value_thb, -1) < 0;

-- 3) Weight by fund should not exceed 100% materially
SELECT
  'fund_weight_sum_le_100' AS check_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS observed,
  0 AS expected,
  'sum(true_weight_pct) per fund <= 100.5' AS detail
FROM (
  SELECT fund_code, SUM(COALESCE(true_weight_pct, 0)) AS sum_weight
  FROM fact_effective_exposure_stock
  GROUP BY fund_code
  HAVING SUM(COALESCE(true_weight_pct, 0)) > 100.5
) t;

-- 4) Coverage ratio in [0,1]
SELECT
  'coverage_ratio_in_range' AS check_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS observed,
  0 AS expected,
  'agg_fund_coverage.coverage_ratio between 0 and 1' AS detail
FROM agg_fund_coverage
WHERE coverage_ratio < 0 OR coverage_ratio > 1;

-- 5) TopN rank uniqueness
SELECT
  'topn_rank_unique' AS check_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS observed,
  0 AS expected,
  'no duplicated rank_no in agg_top_holdings_topn' AS detail
FROM (
  SELECT rank_no
  FROM agg_top_holdings_topn
  GROUP BY rank_no
  HAVING COUNT(*) > 1
) d;

-- 6) Dashboard card total matches fact sum (small tolerance)
SELECT
  'dashboard_total_matches_fact' AS check_name,
  CASE WHEN ABS(a.total_holdings_value_thb - b.fact_total) <= 1.0 THEN 'PASS' ELSE 'FAIL' END AS status,
  ABS(a.total_holdings_value_thb - b.fact_total) AS observed,
  1.0 AS expected,
  'abs(dashboard_total - sum(fact_true_value)) <= 1 THB' AS detail
FROM (
  SELECT COALESCE(total_holdings_value_thb, 0) AS total_holdings_value_thb
  FROM agg_dashboard_cards
  LIMIT 1
) a
CROSS JOIN (
  SELECT COALESCE(SUM(true_value_thb), 0) AS fact_total
  FROM fact_effective_exposure_stock
) b;

-- 7) Dashboard card has exactly one row
SELECT
  'dashboard_cards_single_row' AS check_name,
  CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS observed,
  1 AS expected,
  'agg_dashboard_cards should have exactly 1 row' AS detail
FROM agg_dashboard_cards;
