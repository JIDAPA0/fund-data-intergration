from __future__ import annotations

import pandas as pd
import pymysql
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from .config import FX_TABLE
from .models import Dataset


def create_db_if_needed(db_uri: str) -> None:
    url = make_url(db_uri)
    db_name = url.database
    conn = pymysql.connect(
        host=url.host or "127.0.0.1",
        port=int(url.port or 3306),
        user=url.username or "root",
        password=url.password or "",
        autocommit=True,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
    finally:
        conn.close()


def load_df(engine: Engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def table_exists(engine: Engine, table_name: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchone()
    return bool(row and int(row[0]) > 0)


def table_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return {str(r[0]).lower() for r in rows}


def load_source_data(thai_engine: Engine, global_engine: Engine, fx_engine: Engine) -> Dataset:
    thai_funds = load_df(
        thai_engine,
        """
        SELECT fund_code, full_name_th, full_name_en, amc, category, currency, country
        FROM funds_master_info
        """,
    )

    thai_isin = load_df(
        thai_engine,
        """
        SELECT fund_code, UPPER(TRIM(code)) AS isin_code
        FROM funds_codes
        WHERE type = 'ISIN' AND code IS NOT NULL AND TRIM(code) <> ''
        """,
    )

    thai_nav_aum = load_df(
        thai_engine,
        """
        WITH ranked AS (
            SELECT
                fund_code,
                nav_date,
                aum,
                ROW_NUMBER() OVER (
                    PARTITION BY fund_code
                    ORDER BY (aum IS NOT NULL) DESC, nav_date DESC
                ) AS rn
            FROM funds_daily
        )
        SELECT fund_code, nav_date AS nav_as_of_date, aum
        FROM ranked
        WHERE rn = 1
        """,
    )

    thai_feeder = load_df(
        thai_engine,
        """
        WITH latest AS (
            SELECT fund_code, MAX(as_of_date) AS as_of_date
            FROM funds_holding
            GROUP BY fund_code
        )
        SELECT
            h.fund_code,
            h.name AS feeder_name,
            h.percent AS feeder_weight_pct,
            h.as_of_date,
            h.source_url
        FROM funds_holding h
        JOIN latest l
          ON l.fund_code = h.fund_code
         AND l.as_of_date = h.as_of_date
        WHERE h.type = 'Fund'
        """,
    )

    ft_static = load_df(
        global_engine,
        """
        WITH ranked AS (
            SELECT
                ft_ticker,
                ticker,
                name,
                ticker_type,
                UPPER(TRIM(isin_number)) AS isin_number,
                date_scraper,
                assets_aum_full_value,
                ROW_NUMBER() OVER (
                    PARTITION BY ft_ticker
                    ORDER BY date_scraper DESC, created_at DESC
                ) AS rn
            FROM ft_static_detail
            WHERE ft_ticker IS NOT NULL
        )
        SELECT
            ft_ticker,
            ticker,
            name,
            ticker_type,
            isin_number,
            date_scraper,
            assets_aum_full_value
        FROM ranked
        WHERE rn = 1
        """,
    )

    ft_holdings = load_df(
        global_engine,
        """
        WITH latest AS (
            SELECT ticker, MAX(date_scraper) AS date_scraper
            FROM ft_holdings
            GROUP BY ticker
        )
        SELECT
            h.ticker,
            h.holding_name,
            h.holding_ticker,
            h.holding_type,
            h.portfolio_weight_pct,
            h.date_scraper
        FROM ft_holdings h
        JOIN latest l
          ON l.ticker = h.ticker
         AND l.date_scraper = h.date_scraper
        WHERE h.allocation_type = 'top_10_holdings'
        """,
    )

    sector_cols = table_columns(global_engine, "ft_sector_allocation")
    sector_category_col = "category_name" if "category_name" in sector_cols else "sector_name"
    sector_weight_col = "weight_pct" if "weight_pct" in sector_cols else "sector_weight_pct"
    ft_sector = load_df(
        global_engine,
        f"""
        WITH latest AS (
            SELECT ticker, MAX(date_scraper) AS date_scraper
            FROM ft_sector_allocation
            GROUP BY ticker
        )
        SELECT
            a.ticker,
            a.{sector_category_col} AS category_name,
            a.{sector_weight_col} AS weight_pct,
            a.date_scraper
        FROM ft_sector_allocation a
        JOIN latest l
          ON l.ticker = a.ticker
         AND l.date_scraper = a.date_scraper
        """,
    )

    region_cols = table_columns(global_engine, "ft_region_allocation")
    region_category_col = "category_name" if "category_name" in region_cols else "region_name"
    region_weight_col = "weight_pct" if "weight_pct" in region_cols else "region_weight_pct"
    ft_region = load_df(
        global_engine,
        f"""
        WITH latest AS (
            SELECT ticker, MAX(date_scraper) AS date_scraper
            FROM ft_region_allocation
            GROUP BY ticker
        )
        SELECT
            a.ticker,
            a.{region_category_col} AS category_name,
            a.{region_weight_col} AS weight_pct,
            a.date_scraper
        FROM ft_region_allocation a
        JOIN latest l
          ON l.ticker = a.ticker
         AND l.date_scraper = a.date_scraper
        """,
    )

    return_cols = table_columns(global_engine, "ft_avg_fund_return")
    has_ft_ticker = "ft_ticker" in return_cols
    return_key = "ft_ticker" if has_ft_ticker else "ticker"
    return_date_col = "date_scraper" if "date_scraper" in return_cols else "as_of_date"
    return_1y_col = "avg_fund_return_1y" if "avg_fund_return_1y" in return_cols else "avg_return_1y_pct"
    return_3y_col = "avg_fund_return_3y" if "avg_fund_return_3y" in return_cols else "NULL"
    return_created_col = "created_at" if "created_at" in return_cols else return_date_col

    ft_return = load_df(
        global_engine,
        f"""
        WITH ranked AS (
            SELECT
                {return_key} AS key_ticker,
                ticker,
                {return_1y_col} AS avg_fund_return_1y,
                {return_3y_col} AS avg_fund_return_3y,
                {return_date_col} AS date_scraper,
                ROW_NUMBER() OVER (
                    PARTITION BY {return_key}
                    ORDER BY {return_date_col} DESC, {return_created_col} DESC
                ) AS rn
            FROM ft_avg_fund_return
        )
        SELECT
            key_ticker AS ft_ticker,
            ticker,
            avg_fund_return_1y,
            avg_fund_return_3y,
            date_scraper
        FROM ranked
        WHERE rn = 1
        """,
    )

    if table_exists(fx_engine, FX_TABLE):
        fx_rates = load_df(
            fx_engine,
            f"""
            SELECT
                date_rate,
                UPPER(TRIM(from_ccy)) AS from_ccy,
                UPPER(TRIM(to_ccy)) AS to_ccy,
                rate_to_thb,
                source_system
            FROM {FX_TABLE}
            WHERE to_ccy = 'THB'
            """,
        )
    else:
        fx_rates = pd.DataFrame(
            columns=["date_rate", "from_ccy", "to_ccy", "rate_to_thb", "source_system"]
        )

    return Dataset(
        thai_funds=thai_funds,
        thai_isin=thai_isin,
        thai_nav_aum=thai_nav_aum,
        thai_feeder=thai_feeder,
        ft_static=ft_static,
        ft_holdings=ft_holdings,
        ft_sector=ft_sector,
        ft_region=ft_region,
        ft_return=ft_return,
        fx_rates=fx_rates,
    )
