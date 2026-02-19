# Fund Data Integration (Prefect + MySQL)

This repository is set up for automatic pipeline execution after cloning, using **Prefect** and **MySQL**.

## What the pipeline does

1. (Optional) Imports Thai dump SQL into `raw_thai_funds`
2. Rebuilds mart tables/views (`etl/jobs/build_traceability_mart.py`)
3. Applies API views (`sql/api/funds_API.sql`)
4. (Optional, demo only) Exports dashboard payload JSON (`etl/jobs/export_dashboard_payload.py`)

## Prerequisites

- Python 3.11+
- MySQL running with accessible hosts/ports
- Source DBs available:
  - `raw_ft` on `3306` (global source)
  - `raw_thai_funds` on `3307` (thai source) or provide dump file

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

3. One-time run (manual)

```bash
python -c "from infra.pipelines.prefect_pipeline import fund_data_auto_pipeline; fund_data_auto_pipeline(import_thai_dump=False)"
```

4. (Optional but recommended for API table mode) load ready API data

```bash
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

## Optional: Re-import Thai dump before pipeline

```bash
python -c "from infra.pipelines.prefect_pipeline import fund_data_auto_pipeline; fund_data_auto_pipeline(import_thai_dump=True, thai_dump_path='data/dumps/อะไรก็ได้ที่ไม่เหมือนเดิม.sql')"
```

## Validation (PASS/FAIL)

Run sanity checks against mart database:

```bash
python etl/tools/sanity_check_traceability.py --host 127.0.0.1 --port 3307 --user root --password '' --database fund_traceability
```

Run SQL checks directly:

```bash
python etl/tools/mysql_apply_sql.py sql/validation/traceability_validation.sql --host 127.0.0.1 --port 3307 --user root --password ''
```

## Useful scripts

- `etl/tools/mysql_apply_sql.py` -> apply any SQL file with MySQL multi-statements
- `etl/tools/import_sql_dump.py` -> stream-import large SQL dump into target DB
- `etl/tools/sanity_check_traceability.py` -> one-shot PASS/FAIL validation for mart outputs
- `infra/pipelines/prefect_pipeline.py` -> main orchestrated Prefect flow
- `etl/jobs/build_traceability_mart.py` -> build mart tables/views
- `etl/jobs/export_dashboard_payload.py` -> export payload for demo dashboard
- `API.py` -> FastAPI endpoints (`uvicorn API:app --reload --port 8000`)

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
