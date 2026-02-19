#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import UTC, date, datetime

import pymysql

DEFAULT_API_URL = "https://open.er-api.com/v6/latest/USD"
DEFAULT_SYMBOLS = "THB,USD,EUR,JPY,GBP,CHF,AUD,CAD,CNY,HKD,SGD"


def fetch_rates(api_url: str) -> tuple[datetime, dict[str, float], str]:
    with urllib.request.urlopen(api_url, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    rates = payload.get("rates") or {}
    if not isinstance(rates, dict) or "THB" not in rates:
        raise ValueError("Invalid FX payload: 'rates' missing or THB not present")

    update_unix = payload.get("time_last_update_unix")
    if update_unix is None:
        as_of = datetime.now(tz=UTC)
    else:
        as_of = datetime.fromtimestamp(int(update_unix), tz=UTC)

    source = payload.get("provider") or payload.get("documentation") or "open.er-api.com"
    return as_of, {k.upper(): float(v) for k, v in rates.items()}, str(source)


def ensure_table(conn: pymysql.Connection, table: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
      date_rate DATE NOT NULL,
      from_ccy VARCHAR(10) NOT NULL,
      to_ccy VARCHAR(10) NOT NULL DEFAULT 'THB',
      rate_to_thb DECIMAL(20,8) NOT NULL,
      source_system VARCHAR(100) NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (date_rate, from_ccy, to_ccy),
      KEY idx_fx_from_to_date (from_ccy, to_ccy, date_rate)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def upsert_daily_rates(
    conn: pymysql.Connection,
    table: str,
    as_of_date: str,
    symbols: list[str],
    rates_by_usd: dict[str, float],
    source_system: str,
) -> int:
    thb_per_usd = rates_by_usd.get("THB")
    if thb_per_usd is None or thb_per_usd <= 0:
        raise ValueError("THB rate missing/invalid in API payload")

    rows = []
    for ccy in symbols:
        ccy = ccy.upper()
        if ccy == "THB":
            rate_to_thb = 1.0
        else:
            per_usd = rates_by_usd.get(ccy)
            if per_usd is None or per_usd <= 0:
                continue
            rate_to_thb = thb_per_usd / per_usd
        rows.append((as_of_date, ccy, "THB", float(rate_to_thb), source_system))

    if not rows:
        return 0

    sql = f"""
        INSERT INTO {table} (date_rate, from_ccy, to_ccy, rate_to_thb, source_system)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            rate_to_thb = VALUES(rate_to_thb),
            source_system = VALUES(source_system),
            updated_at = CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def latest_rate_date(conn: pymysql.Connection, table: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(date_rate) AS max_date FROM {table}")
        row = cur.fetchone()
    raw = row[0] if row else None
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw
    return datetime.fromisoformat(str(raw)).date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch daily FX rates and upsert to daily_fx_rates table.")
    parser.add_argument("--host", default=os.getenv("MART_DB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MART_DB_PORT", "3307")))
    parser.add_argument("--user", default=os.getenv("MART_DB_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MART_DB_PASSWORD", ""))
    parser.add_argument("--database", default=os.getenv("MART_DB_NAME", "fund_traceability"))
    parser.add_argument("--table", default=os.getenv("FX_TABLE", "daily_fx_rates"))
    parser.add_argument("--api-url", default=os.getenv("FX_API_URL", DEFAULT_API_URL))
    parser.add_argument("--symbols", default=os.getenv("FX_SYMBOLS", DEFAULT_SYMBOLS))
    parser.add_argument(
        "--stale-max-days",
        type=int,
        default=int(os.getenv("FX_STALE_MAX_DAYS", "3")),
        help="Allow using existing FX table when API fails, if latest date age <= this value",
    )
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if "THB" not in symbols:
        symbols.append("THB")

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        autocommit=False,
        charset="utf8mb4",
    )
    try:
        ensure_table(conn, args.table)
        try:
            as_of_dt, rates, source = fetch_rates(args.api_url)
            as_of_date = as_of_dt.date().isoformat()
            n = upsert_daily_rates(conn, args.table, as_of_date, symbols, rates, source)
            print(f"FX upsert done. date={as_of_date} rows={n} table={args.table}")
            return 0
        except Exception as exc:
            latest = latest_rate_date(conn, args.table)
            if latest is not None:
                age = (datetime.now(tz=UTC).date() - latest).days
                if age <= args.stale_max_days:
                    print(
                        f"FX API unavailable ({exc}); using stale FX data date={latest} age_days={age} "
                        f"(<= {args.stale_max_days})"
                    )
                    return 0
            print(f"FX fetch failed: {exc}", file=sys.stderr)
            return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
