#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

THAI_DB_URI = os.getenv("THAI_DB_URI", "mysql+pymysql://root:@127.0.0.1:3307/raw_thai_funds")
MART_DB_URI = os.getenv("MART_DB_URI", "mysql+pymysql://root:@127.0.0.1:3307/fund_traceability")
OUT_SQL = Path(os.getenv("OUT_SQL", "sql/api/funds_API.sql"))


def q(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def extract_symbol(name: str | None, fallback: str | None = None) -> str | None:
    if fallback and str(fallback).strip():
        return str(fallback).strip().upper()
    if not name:
        return None
    m = re.search(r"\(([A-Za-z0-9._\-]+)\)\s*$", str(name))
    if m:
        return m.group(1).upper()
    m2 = re.search(r"([A-Za-z0-9._\-]{2,})$", str(name).strip())
    return m2.group(1).upper() if m2 else None


def esc(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def insert_block(table: str, cols: list[str], rows: list[tuple], chunk: int = 2000) -> list[str]:
    out = []
    col_sql = ", ".join(cols)
    for i in range(0, len(rows), chunk):
        part = rows[i : i + chunk]
        values = ",\n".join("(" + ", ".join(esc(v) for v in r) + ")" for r in part)
        out.append(f"INSERT INTO {table} ({col_sql}) VALUES\n{values};")
    return out


def main() -> int:
    thai_engine = create_engine(THAI_DB_URI)
    mart_engine = create_engine(MART_DB_URI)

    funds_master = q(
        thai_engine,
        """
        SELECT fund_code, full_name_th, full_name_en, amc, category, risk_level
        FROM funds_master_info
        """,
    )

    fund_return = q(
        thai_engine,
        """
        SELECT fund_code, total_return_1y
        FROM funds_performance
        """,
    )

    latest_aum = q(
        thai_engine,
        """
        WITH ranked AS (
          SELECT fund_code, aum, nav_date,
                 ROW_NUMBER() OVER (PARTITION BY fund_code ORDER BY (aum IS NOT NULL) DESC, nav_date DESC) rn
          FROM funds_daily
        )
        SELECT fund_code, aum
        FROM ranked
        WHERE rn = 1
        """,
    )

    thai_holdings = q(
        thai_engine,
        """
        WITH latest AS (
          SELECT fund_code, MAX(as_of_date) as as_of_date
          FROM funds_holding
          GROUP BY fund_code
        )
        SELECT h.fund_code, h.symbol, h.name AS holding_name, h.sector, h.percent,
               h.type, a.aum
        FROM funds_holding h
        JOIN latest l ON l.fund_code=h.fund_code AND l.as_of_date=h.as_of_date
        LEFT JOIN (
          WITH ranked AS (
            SELECT fund_code, aum, nav_date,
                   ROW_NUMBER() OVER (PARTITION BY fund_code ORDER BY (aum IS NOT NULL) DESC, nav_date DESC) rn
            FROM funds_daily
          )
          SELECT fund_code, aum FROM ranked WHERE rn=1
        ) a ON a.fund_code=h.fund_code
        WHERE h.type LIKE 'Stock%'
        """,
    )

    thai_alloc = q(
        thai_engine,
        """
        WITH latest AS (
          SELECT fund_code, type, MAX(as_of_date) AS as_of_date
          FROM funds_allocations
          WHERE type IN ('sector_alloc', 'country_alloc')
          GROUP BY fund_code, type
        )
        SELECT a.fund_code, a.type, a.name, a.percent
        FROM funds_allocations a
        JOIN latest l
          ON l.fund_code = a.fund_code
         AND l.type = a.type
         AND l.as_of_date = a.as_of_date
        WHERE a.name IS NOT NULL AND TRIM(a.name) <> ''
        """,
    )

    fx_holdings = q(
        mart_engine,
        """
        SELECT
          fund_code,
          holding_name,
          holding_ticker,
          holding_type,
          holding_ticker_norm AS symbol,
          true_weight_pct AS pct_nav,
          true_value_thb AS holding_value_thb,
          aum AS nav_thb,
          map_method
        FROM fact_effective_exposure_stock
        WHERE holding_ticker_norm IS NOT NULL
          AND holding_ticker_norm <> ''
        """,
    )

    exchange_country = {
        "NSQ": "USA",
        "NYQ": "USA",
        "NMQ": "USA",
        "ASE": "USA",
        "PCX": "USA",
        "LSE": "United Kingdom",
        "HKG": "Hong Kong",
        "TAI": "Taiwan",
        "TYO": "Japan",
        "SHH": "China",
        "SHZ": "China",
        "SHE": "China",
        "FRA": "Germany",
        "SWX": "Switzerland",
        "STO": "Sweden",
        "TOR": "Canada",
        "PAR": "France",
        "MIL": "Italy",
        "BRN": "Switzerland",
    }

    def infer_country_from_ticker(raw_ticker: str | None) -> str:
        if not raw_ticker:
            return "GLOBAL"
        s = str(raw_ticker).strip().upper()
        if ":" not in s:
            return "GLOBAL"
        ex = s.rsplit(":", 1)[-1]
        return exchange_country.get(ex, "GLOBAL")

    def infer_sector_from_type(holding_type: str | None) -> str:
        h = (holding_type or "").strip().lower()
        if "stock" in h or "equity" in h:
            return "Equity"
        if "bond" in h or "treasury" in h:
            return "Fixed Income"
        if "cash" in h:
            return "Cash"
        if "reit" in h or "property" in h:
            return "Real Estate"
        if "future" in h or "derivative" in h:
            return "Derivative"
        return "Other"

    # Build funds dimension
    funds = funds_master.merge(fund_return, on="fund_code", how="left")
    funds["return_1y"] = funds["total_return_1y"].fillna(0.0)
    funds["code"] = funds["fund_code"]
    funds["name_th"] = funds["full_name_th"].fillna(funds["fund_code"])
    funds["name_en"] = funds["full_name_en"].fillna(funds["name_th"])
    funds["risk_level"] = pd.to_numeric(funds["risk_level"], errors="coerce").fillna(0).astype(int)

    funds = funds[["code", "name_th", "name_en", "amc", "category", "risk_level", "return_1y"]].copy()

    # Ensure every fund in holdings exists
    all_fund_codes = set(thai_holdings["fund_code"].dropna().astype(str)).union(set(fx_holdings["fund_code"].dropna().astype(str)))
    existing = set(funds["code"].astype(str))
    missing = sorted(all_fund_codes - existing)
    if missing:
        funds = pd.concat(
            [
                funds,
                pd.DataFrame(
                    {
                        "code": missing,
                        "name_th": missing,
                        "name_en": missing,
                        "amc": [None] * len(missing),
                        "category": [None] * len(missing),
                        "risk_level": [0] * len(missing),
                        "return_1y": [0.0] * len(missing),
                    }
                ),
            ],
            ignore_index=True,
        )

    funds = funds.drop_duplicates(subset=["code"]).reset_index(drop=True)
    funds.insert(0, "id", range(1, len(funds) + 1))
    fund_id_map = dict(zip(funds["code"], funds["id"]))

    # Thai holdings -> fact
    th = thai_holdings.copy()
    th["symbol_norm"] = [extract_symbol(n, s) for n, s in zip(th["holding_name"], th["symbol"])]
    th = th[th["symbol_norm"].notna()].copy()
    th["nav_thb"] = pd.to_numeric(th["aum"], errors="coerce").fillna(0.0)
    th["pct_nav"] = pd.to_numeric(th["percent"], errors="coerce").fillna(0.0)
    th["holding_value_thb"] = (th["nav_thb"] * th["pct_nav"] / 100.0).fillna(0.0)
    th["stock_type"] = "TH"
    th["country"] = "THAILAND"
    th["sector"] = th["sector"].fillna("Other")
    th["investment_method"] = "Direct"

    th_fact = th[
        [
            "fund_code",
            "symbol_norm",
            "holding_name",
            "stock_type",
            "country",
            "sector",
            "pct_nav",
            "holding_value_thb",
            "nav_thb",
            "investment_method",
        ]
    ].rename(columns={"symbol_norm": "symbol", "fund_code": "code"})

    # Global holdings -> fact
    gx = fx_holdings.copy()
    gx["symbol"] = gx["symbol"].astype(str).str.upper().str.strip()
    gx = gx[gx["symbol"].str.len() > 0].copy()
    gx["stock_type"] = "FOREIGN"
    gx["country"] = gx["holding_ticker"].map(infer_country_from_ticker)
    gx["sector"] = gx["holding_type"].map(infer_sector_from_type)
    gx["pct_nav"] = pd.to_numeric(gx["pct_nav"], errors="coerce").fillna(0.0)
    gx["holding_value_thb"] = pd.to_numeric(gx["holding_value_thb"], errors="coerce").fillna(0.0)
    gx["nav_thb"] = pd.to_numeric(gx["nav_thb"], errors="coerce").fillna(0.0)
    gx["investment_method"] = gx["map_method"].map(lambda x: "Feeder Fund" if str(x).lower() in {"isin", "name"} else "Other")

    gx_fact = gx[
        [
            "fund_code",
            "symbol",
            "holding_name",
            "stock_type",
            "country",
            "sector",
            "pct_nav",
            "holding_value_thb",
            "nav_thb",
            "investment_method",
        ]
    ].rename(columns={"fund_code": "code"})

    fact = pd.concat([th_fact, gx_fact], ignore_index=True)
    fact = fact[fact["code"].isin(fund_id_map.keys())]

    # Normalize duplicate (fund,symbol,method)
    agg = (
        fact.groupby(["code", "symbol", "holding_name", "stock_type", "country", "sector", "investment_method"], as_index=False)
        .agg({"holding_value_thb": "sum", "pct_nav": "sum", "nav_thb": "max"})
    )

    # ranking per fund
    agg = agg.sort_values(["code", "holding_value_thb"], ascending=[True, False]).reset_index(drop=True)
    agg["ranking"] = agg.groupby("code").cumcount() + 1

    # stocks dimension
    stocks = agg[["symbol", "holding_name", "sector", "stock_type", "country"]].drop_duplicates(subset=["symbol"]).copy()
    stocks = stocks.rename(columns={"holding_name": "full_name"})
    stocks.insert(0, "id", range(1, len(stocks) + 1))
    stock_id_map = dict(zip(stocks["symbol"], stocks["id"]))

    # stock aggregates
    sa = (
        agg.groupby("symbol", as_index=False)
        .agg(total_exposure_value=("holding_value_thb", "sum"), portfolio_weight=("pct_nav", "sum"), total_funds_holding=("code", "nunique"))
    )
    th_sum = agg[agg["stock_type"] == "TH"].groupby("symbol", as_index=False)["holding_value_thb"].sum().rename(columns={"holding_value_thb": "total_thai_fund_value"})
    fx_sum = agg[agg["stock_type"] == "FOREIGN"].groupby("symbol", as_index=False)["holding_value_thb"].sum().rename(columns={"holding_value_thb": "global_fund_value"})

    sa = sa.merge(th_sum, on="symbol", how="left").merge(fx_sum, on="symbol", how="left")
    sa["total_thai_fund_value"] = sa["total_thai_fund_value"].fillna(0.0)
    sa["global_fund_value"] = sa["global_fund_value"].fillna(0.0)
    # Compatibility for clients that always read total_thai_fund_value:
    # for foreign symbols, fallback to global_fund_value so it is not zero.
    stock_type_map = dict(zip(stocks["symbol"], stocks["stock_type"]))
    sa["stock_type"] = sa["symbol"].map(stock_type_map).fillna("FOREIGN")
    sa.loc[
        (sa["stock_type"] == "FOREIGN") & (sa["total_thai_fund_value"] <= 0) & (sa["global_fund_value"] > 0),
        "total_thai_fund_value",
    ] = sa["global_fund_value"]
    sa = sa.drop(columns=["stock_type"])
    sa["exposure_type"] = "Computed"
    sa["stock_id"] = sa["symbol"].map(stock_id_map)
    sa = sa[sa["stock_id"].notna()].copy()
    sa.insert(0, "id", range(1, len(sa) + 1))

    # fund holdings table
    fh = agg.copy()
    fh["fund_id"] = fh["code"].map(fund_id_map)
    fh["stock_id"] = fh["symbol"].map(stock_id_map)
    fh = fh[fh["fund_id"].notna() & fh["stock_id"].notna()].copy()
    fh.insert(0, "id", range(1, len(fh) + 1))

    # fund sector / country breakdown
    alloc = thai_alloc.copy()
    alloc["name"] = alloc["name"].astype(str).str.strip()
    alloc["percentage"] = pd.to_numeric(alloc["percent"], errors="coerce").fillna(0.0)
    alloc["percentage"] = alloc["percentage"].clip(lower=0, upper=100)
    alloc["fund_id"] = alloc["fund_code"].map(fund_id_map)
    alloc = alloc[alloc["fund_id"].notna()].copy()

    fsb = (
        alloc[alloc["type"] == "sector_alloc"]
        .groupby(["fund_id", "name"], as_index=False)["percentage"]
        .sum()
    )
    fsb["percentage"] = fsb["percentage"].clip(lower=0, upper=100)
    fsb.insert(0, "id", range(1, len(fsb) + 1))

    fcb = (
        alloc[alloc["type"] == "country_alloc"]
        .groupby(["fund_id", "name"], as_index=False)["percentage"]
        .sum()
    )
    fcb["percentage"] = fcb["percentage"].clip(lower=0, upper=100)
    fcb.insert(0, "id", range(1, len(fcb) + 1))

    sql_lines = [
        "CREATE USER IF NOT EXISTS 'fund_master'@'%' IDENTIFIED BY 'password';",
        "GRANT ALL PRIVILEGES ON *.* TO 'fund_master'@'%' WITH GRANT OPTION;",
        "FLUSH PRIVILEGES;",
        "",
        "CREATE DATABASE IF NOT EXISTS funds_API;",
        "USE funds_API;",
        "",
        "SET NAMES utf8mb4;",
        "",
        "DROP TABLE IF EXISTS fund_country_breakdown;",
        "DROP TABLE IF EXISTS fund_sector_breakdown;",
        "DROP TABLE IF EXISTS fund_holdings;",
        "DROP TABLE IF EXISTS stock_aggregates;",
        "DROP TABLE IF EXISTS funds;",
        "DROP TABLE IF EXISTS stocks;",
        "",
        "CREATE TABLE stocks ("
        " id INT AUTO_INCREMENT PRIMARY KEY,"
        " symbol VARCHAR(50) NOT NULL UNIQUE,"
        " full_name VARCHAR(255),"
        " sector VARCHAR(100),"
        " stock_type ENUM('TH', 'FOREIGN', 'GOLD') DEFAULT 'FOREIGN',"
        " percent_change DECIMAL(5, 2) DEFAULT 0.00,"
        " country VARCHAR(100) DEFAULT 'USA'"
        ");",
        "CREATE TABLE funds ("
        " id INT AUTO_INCREMENT PRIMARY KEY,"
        " name_th VARCHAR(255) NOT NULL,"
        " name_en VARCHAR(255),"
        " amc VARCHAR(100),"
        " category VARCHAR(100),"
        " code VARCHAR(50),"
        " risk_level INT,"
        " return_1y DECIMAL(5, 2) DEFAULT 0.00"
        ");",
        "CREATE TABLE stock_aggregates ("
        " id INT AUTO_INCREMENT PRIMARY KEY,"
        " stock_id INT NOT NULL,"
        " total_exposure_value DECIMAL(20, 2),"
        " portfolio_weight DECIMAL(12, 4),"
        " exposure_type VARCHAR(100),"
        " total_funds_holding INT,"
        " total_thai_fund_value DECIMAL(20, 2),"
        " global_fund_value DECIMAL(20, 2),"
        " FOREIGN KEY (stock_id) REFERENCES stocks(id)"
        ");",
        "CREATE TABLE fund_holdings ("
        " id INT AUTO_INCREMENT PRIMARY KEY,"
        " fund_id INT NOT NULL,"
        " stock_id INT NOT NULL,"
        " ranking INT,"
        " investment_method ENUM('Direct', 'Feeder Fund', 'Other'),"
        " holding_value_thb DECIMAL(20, 2),"
        " nav_thb DECIMAL(20, 2),"
        " percent_nav DECIMAL(5, 2),"
        " updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        " FOREIGN KEY (fund_id) REFERENCES funds(id),"
        " FOREIGN KEY (stock_id) REFERENCES stocks(id)"
        ");",
        "CREATE TABLE fund_sector_breakdown ("
        " id INT AUTO_INCREMENT PRIMARY KEY,"
        " fund_id INT NOT NULL,"
        " sector_name VARCHAR(100) NOT NULL,"
        " percentage DECIMAL(5, 2) DEFAULT 0.00,"
        " updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        " FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE"
        ");",
        "CREATE TABLE fund_country_breakdown ("
        " id INT AUTO_INCREMENT PRIMARY KEY,"
        " fund_id INT NOT NULL,"
        " country_name VARCHAR(100) NOT NULL,"
        " percentage DECIMAL(5, 2) DEFAULT 0.00,"
        " updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        " FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE"
        ");",
        "",
    ]

    stocks_rows = [
        (
            int(r.id),
            r.symbol,
            (r.full_name or r.symbol)[:255],
            (r.sector or "Other")[:100],
            r.stock_type if r.stock_type in {"TH", "FOREIGN", "GOLD"} else "FOREIGN",
            0.0,
            (r.country or "GLOBAL")[:100],
        )
        for r in stocks.itertuples(index=False)
    ]
    funds_rows = [
        (
            int(r.id),
            (r.name_th or r.code)[:255],
            (r.name_en or r.name_th or r.code)[:255],
            (r.amc[:100] if isinstance(r.amc, str) else None),
            (r.category[:100] if isinstance(r.category, str) else None),
            r.code,
            int(r.risk_level) if pd.notna(r.risk_level) else 0,
            float(r.return_1y) if pd.notna(r.return_1y) else 0.0,
        )
        for r in funds.itertuples(index=False)
    ]
    sa_rows = [
        (
            int(r.id),
            int(r.stock_id),
            float(r.total_exposure_value),
            float(r.portfolio_weight),
            r.exposure_type,
            int(r.total_funds_holding),
            float(r.total_thai_fund_value),
            float(r.global_fund_value),
        )
        for r in sa.itertuples(index=False)
    ]
    fh_rows = [
        (
            int(r.id),
            int(r.fund_id),
            int(r.stock_id),
            int(r.ranking),
            r.investment_method if r.investment_method in {"Direct", "Feeder Fund", "Other"} else "Other",
            float(r.holding_value_thb),
            float(r.nav_thb),
            float(round(r.pct_nav, 2)),
        )
        for r in fh.itertuples(index=False)
    ]
    fsb_rows = [
        (
            int(r.id),
            int(r.fund_id),
            str(r.name)[:100],
            float(round(r.percentage, 2)),
        )
        for r in fsb.itertuples(index=False)
    ]
    fcb_rows = [
        (
            int(r.id),
            int(r.fund_id),
            str(r.name)[:100],
            float(round(r.percentage, 2)),
        )
        for r in fcb.itertuples(index=False)
    ]

    sql_lines.extend(insert_block("stocks", ["id", "symbol", "full_name", "sector", "stock_type", "percent_change", "country"], stocks_rows))
    sql_lines.append("")
    sql_lines.extend(insert_block("funds", ["id", "name_th", "name_en", "amc", "category", "code", "risk_level", "return_1y"], funds_rows))
    sql_lines.append("")
    sql_lines.extend(
        insert_block(
            "stock_aggregates",
            ["id", "stock_id", "total_exposure_value", "portfolio_weight", "exposure_type", "total_funds_holding", "total_thai_fund_value", "global_fund_value"],
            sa_rows,
        )
    )
    sql_lines.append("")
    sql_lines.extend(
        insert_block(
            "fund_holdings",
            ["id", "fund_id", "stock_id", "ranking", "investment_method", "holding_value_thb", "nav_thb", "percent_nav"],
            fh_rows,
        )
    )
    sql_lines.append("")
    sql_lines.extend(insert_block("fund_sector_breakdown", ["id", "fund_id", "sector_name", "percentage"], fsb_rows))
    sql_lines.append("")
    sql_lines.extend(insert_block("fund_country_breakdown", ["id", "fund_id", "country_name", "percentage"], fcb_rows))
    sql_lines.append("")
    sql_lines.append("CREATE INDEX idx_stock_symbol ON stocks(symbol);")
    sql_lines.append("CREATE INDEX idx_fund_code ON funds(code);")

    OUT_SQL.write_text("\n".join(sql_lines), encoding="utf-8")

    print(f"Wrote {OUT_SQL}")
    print(
        " ".join(
            [
                f"stocks={len(stocks_rows)}",
                f"funds={len(funds_rows)}",
                f"stock_aggregates={len(sa_rows)}",
                f"fund_holdings={len(fh_rows)}",
                f"fund_sector_breakdown={len(fsb_rows)}",
                f"fund_country_breakdown={len(fcb_rows)}",
            ]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
