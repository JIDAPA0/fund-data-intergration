# Fund Data Integration (Prefect + MySQL)

This repository is set up for automatic pipeline execution after cloning, using **Prefect** and **MySQL**.

## What the pipeline does

1. Rebuilds mart tables/views (`etl/jobs/build_traceability_mart.py`)
2. Runs data-quality checks (`etl/tools/sanity_check_traceability.py`)
3. Applies API views (`sql/api/funds_API.sql`)
4. (Optional, demo only) Exports dashboard payload JSON (`etl/jobs/export_dashboard_payload.py`)

## Prerequisites

- Python 3.11+
- MySQL running with accessible hosts/ports
- Source DBs available:
  - `raw_ft` (global source)
  - `raw_thai_funds` (thai source)

## Quick Start

1. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configure environment

```bash
cp .env.example .env
# then edit .env values to match your machine
```

Required for DB connections:
- `THAI_DB_URI`
- `GLOBAL_DB_URI`
- `MART_DB_URI`

Optional for FX conversion to THB:
- `FX_DB_URI` (defaults to `MART_DB_URI`)
- `FX_TABLE` (defaults to `daily_fx_rates`)
- `FX_BASE_CCY` (defaults to `THB`)
- `FX_API_URL` (fixed provider in this project: `https://open.er-api.com/v6/latest/USD`)
- `FX_SYMBOLS` (comma-separated symbols to ingest daily)
- `FX_STALE_MAX_DAYS` (allow pipeline to proceed on API outage if latest FX data is not older than this)
- `FX_MISSING_MAX_PCT` (alert threshold for `default_1_missing_fx` in sanity check)

3. Build mart (main flow: DB already exists)

```bash
python etl/jobs/build_traceability_mart.py
```

4. Validate PASS/FAIL

```bash
python etl/tools/sanity_check_traceability.py --host 127.0.0.1 --port 3307 --user root --password '' --database fund_traceability
```

5. (Optional) Build/apply API SQL snapshot

```bash
python etl/tools/build_funds_api_sql.py
python etl/tools/mysql_apply_sql.py sql/api/funds_API.sql --host 127.0.0.1 --port 3307 --user root --password ''
```

## Prefect Auto Mode

The flow is configured with cron:

- `0 */6 * * *` (every 6 hours)

Start it with:

```bash
python infra/pipelines/prefect_pipeline.py
```

If you want to run without scheduler (single run), use Python directly:

```bash
python -c "from infra.pipelines.prefect_pipeline import fund_data_auto_pipeline; fund_data_auto_pipeline(import_thai_dump=False)"
```

## Optional: Load Source Data from SQL Dumps

If source DBs are not ready, import dump files first.

Thai source (`raw_thai_funds`):

```bash
python etl/tools/import_sql_dump.py data/dumps/thai.sql --target-db raw_thai_funds --host 127.0.0.1 --port 3307 --user root --password ''
```

Global source (`raw_ft`):

```bash
python etl/tools/import_sql_dump.py data/dumps/global.sql --target-db raw_ft --host 127.0.0.1 --port 3306 --user root --password ''
```

## Optional: Enable FX Conversion (to THB)

Create FX schema table:

```bash
python etl/tools/mysql_apply_sql.py sql/schema/fx_rates_schema.sql --host 127.0.0.1 --port 3307 --user root --password ''
```

Fetch daily rates from API into `daily_fx_rates`:

```bash
python etl/tools/fetch_daily_fx_rates.py --host 127.0.0.1 --port 3307 --user root --password '' --database fund_traceability
```

Then build. The mart will expose `vw_nav_aum_thb` (NAV converted to THB via SQL JOIN on `daily_fx_rates`).

Fallback policy when FX API is unavailable:
- Keep existing rows in `daily_fx_rates` (no overwrite).
- Continue pipeline only if latest FX snapshot age is within `FX_STALE_MAX_DAYS`.
- Fail pipeline if there is no FX data or data is older than allowed threshold.

## Validation (PASS/FAIL)

Run sanity checks against mart database:

```bash
python etl/tools/sanity_check_traceability.py --host 127.0.0.1 --port 3307 --user root --password '' --database fund_traceability
```

The sanity check now raises FAIL when `% of non-THB rows` with `fx_rate_status='default_1_missing_fx'` exceeds `FX_MISSING_MAX_PCT`.

Run SQL checks directly:

```bash
python etl/tools/mysql_apply_sql.py sql/validation/traceability_validation.sql --host 127.0.0.1 --port 3307 --user root --password ''
```

## Smoke Test

Run one short smoke test (build + key table row-count checks):

```bash
python etl/tools/smoke_test_traceability.py --host 127.0.0.1 --port 3307 --user root --password '' --database fund_traceability
```

## Useful scripts

- `etl/tools/mysql_apply_sql.py` -> apply any SQL file with MySQL multi-statements
- `etl/tools/import_sql_dump.py` -> stream-import large SQL dump into target DB
- `etl/tools/fetch_daily_fx_rates.py` -> fetch and upsert daily FX rates from API
- `etl/tools/sanity_check_traceability.py` -> one-shot PASS/FAIL validation for mart outputs
- `etl/tools/smoke_test_traceability.py` -> run build and verify key table row counts
- `infra/pipelines/prefect_pipeline.py` -> main orchestrated Prefect flow
- `etl/jobs/build_traceability_mart.py` -> build mart tables/views
- `etl/jobs/export_dashboard_payload.py` -> export payload for demo dashboard

## Folder layout

- `artifacts/zips/` -> exported zip packages for sharing
- `reports/exploration/` -> exploration outputs (`explore_raw_*.txt`)
- `data/dumps/` -> large SQL dump files
- `examples/dashboard/` -> example dashboard (non-production)
- `etl/jobs/` -> core ETL jobs (mart + payload)
- `etl/tools/` -> helper tools for SQL import/apply/build
- `infra/pipelines/` -> orchestration flows
- `sql/api/` -> API SQL files
- `sql/validation/` -> SQL validation checks for mart quality
- `sql/schema/` -> schema SQL files
- `docs/` -> additional documentation

## Output

- API views in MySQL schema: `funds_api`
- Dashboard payload file (example only): `examples/dashboard/data/dashboard_data.json`
