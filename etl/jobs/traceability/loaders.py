from __future__ import annotations

import pandas as pd
import pymysql
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

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


def load_source_data(thai_engine: Engine, global_engine: Engine) -> Dataset:
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

    ft_sector = load_df(
        global_engine,
        """
        WITH latest AS (
            SELECT ticker, MAX(date_scraper) AS date_scraper
            FROM ft_sector_allocation
            GROUP BY ticker
        )
        SELECT
            a.ticker,
            a.category_name,
            a.weight_pct,
            a.date_scraper
        FROM ft_sector_allocation a
        JOIN latest l
          ON l.ticker = a.ticker
         AND l.date_scraper = a.date_scraper
        """,
    )

    ft_region = load_df(
        global_engine,
        """
        WITH latest AS (
            SELECT ticker, MAX(date_scraper) AS date_scraper
            FROM ft_region_allocation
            GROUP BY ticker
        )
        SELECT
            a.ticker,
            a.category_name,
            a.weight_pct,
            a.date_scraper
        FROM ft_region_allocation a
        JOIN latest l
          ON l.ticker = a.ticker
         AND l.date_scraper = a.date_scraper
        """,
    )

    ft_return = load_df(
        global_engine,
        """
        WITH ranked AS (
            SELECT
                ft_ticker,
                ticker,
                avg_fund_return_1y,
                avg_fund_return_3y,
                date_scraper,
                ROW_NUMBER() OVER (
                    PARTITION BY ft_ticker
                    ORDER BY date_scraper DESC, created_at DESC
                ) AS rn
            FROM ft_avg_fund_return
        )
        SELECT ft_ticker, ticker, avg_fund_return_1y, avg_fund_return_3y, date_scraper
        FROM ranked
        WHERE rn = 1
        """,
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
    )
