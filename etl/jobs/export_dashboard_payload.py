#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from decimal import Decimal
from datetime import date, datetime
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

DB_HOST = os.getenv("API_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("API_DB_PORT", "3307"))
DB_USER = os.getenv("API_DB_USER", "root")
DB_PASSWORD = os.getenv("API_DB_PASSWORD", "")
DB_NAME = os.getenv("API_DB_NAME", "funds_api")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUT_PATH = Path(os.getenv("OUT_PATH", str(PROJECT_ROOT / "examples" / "dashboard" / "data" / "dashboard_data.json")))


def norm_row(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def fetch_all(cur, sql: str):
    cur.execute(sql)
    return [norm_row(r) for r in cur.fetchall()]


def main() -> int:
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=DictCursor,
    )

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM api_dashboard_summary LIMIT 1")
            dashboard = norm_row(cur.fetchone() or {})

            payload = {
                "dashboard_summary": dashboard,
                "dashboard_summary_thai": fetch_all(cur, "SELECT * FROM api_dashboard_summary_thai LIMIT 1")[0],
                "dashboard_summary_global": fetch_all(cur, "SELECT * FROM api_dashboard_summary_global LIMIT 1")[0],
                "top_thai_holdings_top10": fetch_all(cur, "SELECT * FROM api_top_thai_holdings ORDER BY rank_no"),
                "top_thai_holdings_all": fetch_all(cur, "SELECT * FROM api_top_thai_holdings_all ORDER BY rank_no"),
                "top_global_traceability_top10": fetch_all(cur, "SELECT * FROM api_top_global_traceability ORDER BY rank_no"),
                "top_global_traceability_all": fetch_all(cur, "SELECT * FROM api_top_global_traceability_all ORDER BY rank_no"),
                "sector_allocation_thai": fetch_all(cur, "SELECT * FROM api_sector_allocation_thai ORDER BY total_value_thb DESC"),
                "sector_allocation_global": fetch_all(cur, "SELECT * FROM api_sector_allocation_global ORDER BY total_value_thb DESC"),
                "country_allocation_thai": fetch_all(cur, "SELECT * FROM api_country_allocation_thai ORDER BY total_value_thb DESC"),
                "country_allocation_global": fetch_all(cur, "SELECT * FROM api_country_allocation_global ORDER BY total_value_thb DESC"),
                "search_by_fund": fetch_all(cur, "SELECT * FROM api_search_by_fund ORDER BY total_true_value_thb DESC LIMIT 500"),
                "search_by_asset": fetch_all(cur, "SELECT * FROM api_search_by_asset ORDER BY total_true_value_thb DESC LIMIT 500"),
            }
    finally:
        conn.close()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
