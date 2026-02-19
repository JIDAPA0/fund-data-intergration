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
TOP_N='10'
```

## Quick checks

```sql
SELECT * FROM vw_dashboard_cards;
SELECT * FROM vw_top_holdings;
SELECT * FROM vw_sector_allocation LIMIT 10;
SELECT * FROM vw_country_allocation LIMIT 10;
```
