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

def clean_stock_name(full_name: str, symbol: str) -> str:
    if not full_name or not symbol:
        return full_name
    clean_name = str(full_name).strip()
    sym = str(symbol).strip()
    
    if clean_name.endswith(sym):
        clean_name = clean_name[:-len(sym)].strip()
        
    return re.sub(r'[^A-Za-z0-9\s.,&()\-]', '', clean_name).strip()

def esc(v):
    if v is None or pd.isna(v):
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"

def insert_block(table: str, cols: list[str], rows: list[tuple], chunk: int = 2000) -> list[str]:
    if not rows:
        return []
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

    print("Fetching data from Thai Database...")
    funds_master = q(thai_engine, "SELECT fund_code, full_name_th, full_name_en, amc, category, risk_level FROM funds_master_info")
    fund_return = q(thai_engine, "SELECT fund_code, total_return_1y FROM funds_performance")
    
    latest_aum = q(thai_engine, """
        WITH ranked AS (
          SELECT fund_code, aum, nav_date,
                 ROW_NUMBER() OVER (PARTITION BY fund_code ORDER BY (aum IS NOT NULL) DESC, nav_date DESC) rn
          FROM funds_daily
        )
        SELECT fund_code, aum FROM ranked WHERE rn = 1
    """)

    thai_holdings = q(thai_engine, """
        WITH latest AS (
          SELECT fund_code, MAX(as_of_date) as as_of_date
          FROM funds_holding
          GROUP BY fund_code
        )
        SELECT h.fund_code, h.symbol, h.name AS holding_name, h.sector, h.percent, h.type
        FROM funds_holding h
        JOIN latest l ON l.fund_code=h.fund_code AND l.as_of_date=h.as_of_date
    """)
    thai_holdings = thai_holdings.merge(latest_aum, on="fund_code", how="left")

    thai_alloc = q(thai_engine, """
        WITH latest AS (
          SELECT fund_code, type, MAX(as_of_date) AS as_of_date
          FROM funds_allocations
          WHERE type IN ('sector_alloc', 'country_alloc')
          GROUP BY fund_code, type
        )
        SELECT a.fund_code, a.type, a.name, a.percent
        FROM funds_allocations a
        JOIN latest l ON l.fund_code = a.fund_code AND l.type = a.type AND l.as_of_date = a.as_of_date
        WHERE a.name IS NOT NULL AND TRIM(a.name) <> ''
    """)

    print("Fetching data from Data Mart (Global Exposure)...")
    fx_holdings = q(mart_engine, """
        SELECT
          fund_code,
          holding_name,
          holding_ticker_norm AS symbol,
          true_weight_pct AS pct_nav,
          true_value_thb AS holding_value_thb
        FROM fact_effective_exposure_stock
        WHERE holding_ticker_norm IS NOT NULL AND holding_ticker_norm <> ''
    """)

    print("Processing 3-Tier Data Structures and Allocations...")
    
    stocks_dict = {}
    funds_dict = {}
    master_funds_dict = {}
    
    symbol_to_id = {}
    code_to_fund_id = {}
    master_name_to_id = {}
    
    funds_master = funds_master.merge(fund_return, on="fund_code", how="left")
    fund_id_seq = 1
    for _, row in funds_master.iterrows():
        code = row["fund_code"]
        funds_dict[fund_id_seq] = (
            fund_id_seq, row["full_name_th"], row["full_name_en"], 
            row["amc"], row["category"], code, row["risk_level"], row["total_return_1y"]
        )
        code_to_fund_id[code] = fund_id_seq
        fund_id_seq += 1

    stock_id_seq = 1
    master_id_seq = 1
    
    fund_direct_rows = []
    fund_master_rows = []
    
    for _, row in thai_holdings.iterrows():
        fund_id = code_to_fund_id.get(row["fund_code"])
        if not fund_id: continue
        
        asset_type = str(row["type"]).upper()
        percent = float(row["percent"]) if pd.notna(row["percent"]) else 0.0
        aum = float(row["aum"]) if pd.notna(row["aum"]) else 0.0
        value_thb = (percent * aum) / 100.0
        
        if "FUND" in asset_type or "UNIT" in asset_type or "TRUST" in asset_type:
            m_name = str(row["holding_name"]).strip()
            if m_name not in master_name_to_id:
                master_funds_dict[master_id_seq] = (master_id_seq, m_name, "Global AMC", "Equity")
                master_name_to_id[m_name] = master_id_seq
                master_id_seq += 1
                
            m_id = master_name_to_id[m_name]
            fund_master_rows.append((None, fund_id, m_id, value_thb, percent))
            
        else:
            raw_sym = row["symbol"]
            sym = extract_symbol(raw_sym, raw_sym)
            if not sym: continue
            
            full_name = clean_stock_name(row["holding_name"], sym)
            
            if sym not in symbol_to_id:
                stocks_dict[stock_id_seq] = (stock_id_seq, sym, full_name, row["sector"], "TH", 0.0, "Thailand")
                symbol_to_id[sym] = stock_id_seq
                stock_id_seq += 1
                
            s_id = symbol_to_id[sym]
            fund_direct_rows.append((None, fund_id, s_id, 1, value_thb, aum, percent))

    master_fund_stock_rows = []
    for _, row in fx_holdings.iterrows():
        sym = row["symbol"]
        if sym not in symbol_to_id:
            full_name = clean_stock_name(row["holding_name"], sym)
            stocks_dict[stock_id_seq] = (stock_id_seq, sym, full_name, "Global Sector", "FOREIGN", 0.0, "USA")
            symbol_to_id[sym] = stock_id_seq
            stock_id_seq += 1
            
        s_id = symbol_to_id[sym]
        fund_id = code_to_fund_id.get(row["fund_code"])
        if fund_id:
            matched_master_ids = [m[2] for m in fund_master_rows if m[1] == fund_id]
            if matched_master_ids:
                m_id = matched_master_ids[0]
                pct = float(row["pct_nav"]) if pd.notna(row["pct_nav"]) else 0.0
                if (m_id, s_id) not in [(x[1], x[2]) for x in master_fund_stock_rows]:
                    master_fund_stock_rows.append((None, m_id, s_id, pct))

    fsb_rows = []
    fcb_rows = []
    for _, row in thai_alloc.iterrows():
        fund_id = code_to_fund_id.get(row["fund_code"])
        if not fund_id: continue
        
        alloc_type = str(row["type"]).strip().lower()
        alloc_name = str(row["name"]).strip()
        alloc_pct = float(row["percent"]) if pd.notna(row["percent"]) else 0.0
        
        if alloc_type == 'sector_alloc':
            fsb_rows.append((None, fund_id, alloc_name, alloc_pct))
        elif alloc_type == 'country_alloc':
            fcb_rows.append((None, fund_id, alloc_name, alloc_pct))

    print("Generating SQL File...")
    sql_lines = []
    sql_lines.append("SET NAMES utf8mb4;")
    sql_lines.append("SET FOREIGN_KEY_CHECKS = 0;")
    
    sql_lines.append("""
DROP TABLE IF EXISTS fund_sector_breakdown;
DROP TABLE IF EXISTS fund_country_breakdown;
DROP TABLE IF EXISTS stock_aggregates;
DROP TABLE IF EXISTS master_fund_holdings;
DROP TABLE IF EXISTS fund_master_holdings;
DROP TABLE IF EXISTS fund_direct_holdings;
DROP TABLE IF EXISTS master_funds;
DROP TABLE IF EXISTS funds;
DROP TABLE IF EXISTS stocks;
    """)

    sql_lines.append("""
CREATE TABLE stocks ( id INT PRIMARY KEY, symbol VARCHAR(50) NOT NULL UNIQUE, full_name VARCHAR(255), sector VARCHAR(100), stock_type ENUM('TH', 'FOREIGN', 'GOLD') DEFAULT 'FOREIGN', percent_change DECIMAL(5, 2) DEFAULT 0.00, country VARCHAR(100) DEFAULT 'USA');
CREATE TABLE funds ( id INT PRIMARY KEY, name_th VARCHAR(255) NOT NULL, name_en VARCHAR(255), amc VARCHAR(100), category VARCHAR(100), code VARCHAR(50) UNIQUE, risk_level INT, return_1y DECIMAL(5, 2) DEFAULT 0.00);
CREATE TABLE master_funds ( id INT PRIMARY KEY, name_en VARCHAR(255) NOT NULL UNIQUE, amc VARCHAR(100), category VARCHAR(100));
CREATE TABLE fund_direct_holdings ( id INT AUTO_INCREMENT PRIMARY KEY, fund_id INT NOT NULL, stock_id INT NOT NULL, ranking INT, holding_value_thb DECIMAL(20, 2), nav_thb DECIMAL(20, 2), percent_nav DECIMAL(5, 2));
CREATE TABLE fund_master_holdings ( id INT AUTO_INCREMENT PRIMARY KEY, fund_id INT NOT NULL, master_fund_id INT NOT NULL, holding_value_thb DECIMAL(20, 2), percent_nav DECIMAL(5, 2));
CREATE TABLE master_fund_holdings ( id INT AUTO_INCREMENT PRIMARY KEY, master_fund_id INT NOT NULL, stock_id INT NOT NULL, percent_weight DECIMAL(5, 2));
CREATE TABLE fund_sector_breakdown ( id INT AUTO_INCREMENT PRIMARY KEY, fund_id INT NOT NULL, sector_name VARCHAR(100) NOT NULL, percentage DECIMAL(5, 2) DEFAULT 0.00 );
CREATE TABLE fund_country_breakdown ( id INT AUTO_INCREMENT PRIMARY KEY, fund_id INT NOT NULL, country_name VARCHAR(100) NOT NULL, percentage DECIMAL(5, 2) DEFAULT 0.00 );
    """)

    sql_lines.extend(insert_block("stocks", ["id", "symbol", "full_name", "sector", "stock_type", "percent_change", "country"], list(stocks_dict.values())))
    sql_lines.append("")
    sql_lines.extend(insert_block("funds", ["id", "name_th", "name_en", "amc", "category", "code", "risk_level", "return_1y"], list(funds_dict.values())))
    sql_lines.append("")
    sql_lines.extend(insert_block("master_funds", ["id", "name_en", "amc", "category"], list(master_funds_dict.values())))
    sql_lines.append("")
    sql_lines.extend(insert_block("fund_direct_holdings", ["id", "fund_id", "stock_id", "ranking", "holding_value_thb", "nav_thb", "percent_nav"], fund_direct_rows))
    sql_lines.append("")
    sql_lines.extend(insert_block("fund_master_holdings", ["id", "fund_id", "master_fund_id", "holding_value_thb", "percent_nav"], fund_master_rows))
    sql_lines.append("")
    sql_lines.extend(insert_block("master_fund_holdings", ["id", "master_fund_id", "stock_id", "percent_weight"], master_fund_stock_rows))
    sql_lines.append("")
    sql_lines.extend(insert_block("fund_sector_breakdown", ["id", "fund_id", "sector_name", "percentage"], fsb_rows))
    sql_lines.append("")
    sql_lines.extend(insert_block("fund_country_breakdown", ["id", "fund_id", "country_name", "percentage"], fcb_rows))
    
    sql_lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    sql_lines.append("CREATE INDEX idx_stock_symbol ON stocks(symbol);")
    sql_lines.append("CREATE INDEX idx_fund_code ON funds(code);")

    OUT_SQL.parent.mkdir(parents=True, exist_ok=True)
    OUT_SQL.write_text("\n".join(sql_lines), encoding="utf-8")

    print(f"SQL file generated successfully at: {OUT_SQL}")
    print(f"Metrics -> Stocks: {len(stocks_dict)} | Funds: {len(funds_dict)} | Master Funds: {len(master_funds_dict)}")
    print(f"Metrics -> Direct: {len(fund_direct_rows)} | Feeder: {len(fund_master_rows)} | Master-Stocks: {len(master_fund_stock_rows)}")
    print(f"Metrics -> Sectors: {len(fsb_rows)} | Countries: {len(fcb_rows)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
