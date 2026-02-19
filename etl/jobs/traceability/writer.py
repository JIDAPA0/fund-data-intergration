from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import FX_TABLE


def write_tables(mart_engine: Engine, tables: dict[str, pd.DataFrame]) -> None:
    with mart_engine.begin() as conn:
        for name, df in tables.items():
            df.to_sql(name, conn, if_exists="replace", index=False)


def create_views(mart_engine: Engine) -> None:
    view_sql = [
        f"""
        CREATE TABLE IF NOT EXISTS {FX_TABLE} (
          date_rate DATE NOT NULL,
          from_ccy VARCHAR(10) NOT NULL,
          to_ccy VARCHAR(10) NOT NULL DEFAULT 'THB',
          rate_to_thb DECIMAL(20,8) NOT NULL,
          source_system VARCHAR(100) NULL,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (date_rate, from_ccy, to_ccy),
          KEY idx_fx_from_to_date (from_ccy, to_ccy, date_rate)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        "DROP VIEW IF EXISTS vw_dashboard_cards",
        "DROP VIEW IF EXISTS vw_top_holdings",
        "DROP VIEW IF EXISTS vw_sector_allocation",
        "DROP VIEW IF EXISTS vw_country_allocation",
        "DROP VIEW IF EXISTS vw_search_by_fund",
        "DROP VIEW IF EXISTS vw_search_by_asset",
        "DROP VIEW IF EXISTS vw_nav_aum_thb",
        """
        CREATE VIEW vw_dashboard_cards AS
        SELECT * FROM agg_dashboard_cards
        """,
        """
        CREATE VIEW vw_top_holdings AS
        SELECT rank_no, holding_name, holding_ticker, holding_type, total_true_weight_pct, total_true_value_thb
        FROM agg_top_holdings_topn
        ORDER BY rank_no
        """,
        """
        CREATE VIEW vw_sector_allocation AS
        SELECT sector_name, total_true_weight_pct, total_true_value_thb
        FROM agg_sector_exposure
        ORDER BY total_true_value_thb DESC
        """,
        """
        CREATE VIEW vw_country_allocation AS
        SELECT region_name AS country_name, total_true_weight_pct, total_true_value_thb
        FROM agg_country_exposure
        ORDER BY total_true_value_thb DESC
        """,
        """
        CREATE VIEW vw_search_by_fund AS
        SELECT
            fund_code,
            MIN(holding_name) AS holding_name,
            NULLIF(holding_ticker_norm, '') AS holding_ticker,
            holding_type,
            SUM(true_weight_pct) AS total_true_weight_pct,
            SUM(true_value_thb) AS total_true_value_thb
        FROM fact_effective_exposure_stock
        GROUP BY fund_code, holding_key, holding_ticker_norm, holding_type
        """,
        """
        CREATE VIEW vw_search_by_asset AS
        SELECT
            MIN(holding_name) AS holding_name,
            NULLIF(holding_ticker_norm, '') AS holding_ticker,
            holding_type,
            fund_code,
            SUM(true_weight_pct) AS total_true_weight_pct,
            SUM(true_value_thb) AS total_true_value_thb
        FROM fact_effective_exposure_stock
        GROUP BY holding_key, holding_ticker_norm, holding_type, fund_code
        """,
        f"""
        CREATE VIEW vw_nav_aum_thb AS
        SELECT
            n.fund_code,
            n.nav_as_of_date,
            n.fund_currency,
            n.aum_native,
            CASE
                WHEN n.fund_currency = 'THB' THEN 1.0
                ELSE COALESCE(fx_exact.rate_to_thb, fx_latest.rate_to_thb, 1.0)
            END AS fx_rate_to_thb,
            n.aum_native * (
                CASE
                    WHEN n.fund_currency = 'THB' THEN 1.0
                    ELSE COALESCE(fx_exact.rate_to_thb, fx_latest.rate_to_thb, 1.0)
                END
            ) AS aum_thb,
            CASE
                WHEN n.fund_currency = 'THB' THEN 'base_currency'
                WHEN fx_exact.rate_to_thb IS NOT NULL THEN 'exact'
                WHEN fx_latest.rate_to_thb IS NOT NULL THEN 'latest'
                ELSE 'default_1_missing_fx'
            END AS fx_rate_status
        FROM stg_nav_aum_native n
        LEFT JOIN {FX_TABLE} fx_exact
          ON fx_exact.from_ccy = n.fund_currency
         AND fx_exact.to_ccy = 'THB'
         AND fx_exact.date_rate = n.nav_as_of_date
        LEFT JOIN {FX_TABLE} fx_latest
          ON fx_latest.from_ccy = n.fund_currency
         AND fx_latest.to_ccy = 'THB'
         AND fx_latest.date_rate = (
            SELECT MAX(f2.date_rate)
            FROM {FX_TABLE} f2
            WHERE f2.from_ccy = n.fund_currency
              AND f2.to_ccy = 'THB'
              AND f2.date_rate <= n.nav_as_of_date
         )
        """,
    ]
    with mart_engine.begin() as conn:
        for sql in view_sql:
            conn.execute(text(sql))


def print_summary(tables: dict[str, pd.DataFrame]) -> None:
    cards = tables["agg_dashboard_cards"].iloc[0]
    print("Build complete")
    print("- mapped funds:", int(cards["mapped_fund_count"]))
    print("- mapped masters:", int(cards["mapped_master_count"]))
    print("- total holdings value (THB):", round(float(cards["total_holdings_value_thb"]), 2))
    print("- top sector:", cards["top_sector_name"], round(float(cards["top_sector_weight_pct"] or 0), 4))
    print("- top country:", cards["top_country_name"], round(float(cards["top_country_weight_pct"] or 0), 4))
    print("- avg fund return 1y:", cards["avg_fund_return_1y"])
