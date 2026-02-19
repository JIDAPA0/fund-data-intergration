from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def write_tables(mart_engine: Engine, tables: dict[str, pd.DataFrame]) -> None:
    with mart_engine.begin() as conn:
        for name, df in tables.items():
            df.to_sql(name, conn, if_exists="replace", index=False)


def create_views(mart_engine: Engine) -> None:
    view_sql = [
        "DROP VIEW IF EXISTS vw_dashboard_cards",
        "DROP VIEW IF EXISTS vw_top_holdings",
        "DROP VIEW IF EXISTS vw_sector_allocation",
        "DROP VIEW IF EXISTS vw_country_allocation",
        "DROP VIEW IF EXISTS vw_search_by_fund",
        "DROP VIEW IF EXISTS vw_search_by_asset",
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
