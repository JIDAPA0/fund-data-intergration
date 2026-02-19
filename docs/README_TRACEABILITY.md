# Fund Traceability Calculation System

This project now includes a ready-to-run mart builder for Thai fund traceability and effective exposure.

## What it builds

Target DB: `fund_traceability` (default: MySQL `127.0.0.1:3307`)

Materialized tables:
- `bridge_thai_master`
- `fact_effective_exposure_stock`
- `fact_effective_exposure_sector`
- `fact_effective_exposure_region`
- `agg_top_holdings`
- `agg_top_holdings_topn`
- `agg_sector_exposure`
- `agg_sector_exposure_topn`
- `agg_country_exposure`
- `agg_country_exposure_topn`
- `agg_region_exposure`
- `agg_fund_coverage`
- `agg_dashboard_cards`

Views for dashboard/API:
- `vw_dashboard_cards`
- `vw_top_holdings`
- `vw_sector_allocation`
- `vw_country_allocation`
- `vw_search_by_fund`
- `vw_search_by_asset`

## Calculation logic (effective exposure)

For each Thai fund and mapped master fund:

`true_weight_pct = feeder_weight_pct * master_holding_weight_pct / 100`

`true_value_thb = thai_fund_aum * true_weight_pct / 100`

## Mapping strategy

1. Preferred: parse ISIN from Thai feeder holding text and map to FT master (`feeder_holding_isin`)
2. Fallback: map Thai fund ISIN directly to FT fund (`thai_fund_isin`)

## Run

```bash
.venv/bin/pip install -r requirements.txt
.venv/bin/python etl/jobs/build_traceability_mart.py
```

Optional env vars:

```bash
THAI_DB_URI='mysql+pymysql://root:@127.0.0.1:3307/raw_thai_funds'
GLOBAL_DB_URI='mysql+pymysql://root:@127.0.0.1:3306/raw_ft'
MART_DB_URI='mysql+pymysql://root:@127.0.0.1:3307/fund_traceability'
FX_DB_URI='mysql+pymysql://root:@127.0.0.1:3307/fund_traceability'
FX_TABLE='daily_fx_rates'
FX_BASE_CCY='THB'
FX_API_URL='https://open.er-api.com/v6/latest/USD'
FX_STALE_MAX_DAYS='3'
FX_MISSING_MAX_PCT='5.0'
TOP_N='10'
```

## Quick checks

```sql
SELECT * FROM vw_dashboard_cards;
SELECT * FROM vw_top_holdings;
SELECT * FROM vw_sector_allocation LIMIT 10;
SELECT * FROM vw_country_allocation LIMIT 10;
SELECT * FROM vw_nav_aum_thb LIMIT 10;
```

## Missing Data Policy

- `aum` missing (`NULL`) is treated as `0` for value calculations.
- `true_value_thb` is forced to numeric and missing values become `0`.
- Weight fields used in calculations are coerced to numeric; missing values become `0`.
- `coverage_ratio` is clipped to `[0, 1]`.
- FX conversion is optional:
  - If `daily_fx_rates` exists, non-THB funds are converted to THB before calculating `true_value_thb`.
  - If FX rate is missing, fallback rate `1.0` is used and row is marked with `fx_rate_status='default_1_missing_fx'`.
- FX provider policy:
  - Daily FX fetch uses a single provider: `open.er-api.com`.
  - If provider is unavailable, pipeline can continue only when latest FX snapshot age is within `FX_STALE_MAX_DAYS`; otherwise the run fails.

This policy prevents null-propagation from breaking aggregates and validation, and makes PASS/FAIL checks deterministic.
