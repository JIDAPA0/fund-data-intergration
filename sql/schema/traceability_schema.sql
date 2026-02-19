-- Traceability platform bootstrap schema
-- MySQL 8+

SET NAMES utf8mb4;

CREATE DATABASE IF NOT EXISTS raw_thai_funds CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS raw_ft CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS fund_traceability CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS funds_api CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- =========================================================
-- 1) RAW LAYER
-- =========================================================

USE raw_thai_funds;

CREATE TABLE IF NOT EXISTS raw_thai_funds_master (
  fund_code VARCHAR(64) PRIMARY KEY,
  fund_name_th VARCHAR(255),
  fund_name_en VARCHAR(255),
  amc_name VARCHAR(255),
  category_name VARCHAR(255),
  risk_level INT,
  currency_code VARCHAR(16),
  country_code VARCHAR(32),
  isin_code VARCHAR(32),
  fund_status VARCHAR(64),
  source_url TEXT,
  scraped_at DATETIME NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_thai_funds_daily (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  nav_date DATE NOT NULL,
  nav_value DECIMAL(20,8),
  bid_value DECIMAL(20,8),
  offer_value DECIMAL(20,8),
  aum_thb DECIMAL(24,2),
  source_url TEXT,
  scraped_at DATETIME NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_th_daily (fund_code, nav_date),
  KEY idx_th_daily_fund_date (fund_code, nav_date)
);

CREATE TABLE IF NOT EXISTS raw_thai_funds_holdings (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  as_of_date DATE NOT NULL,
  holding_symbol VARCHAR(128),
  holding_name VARCHAR(512),
  holding_type VARCHAR(128),
  sector_name VARCHAR(255),
  weight_pct DECIMAL(12,6),
  source_url TEXT,
  scraped_at DATETIME NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_th_holdings_fund_date (fund_code, as_of_date),
  KEY idx_th_holdings_symbol (holding_symbol)
);

CREATE TABLE IF NOT EXISTS raw_thai_funds_allocations (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  as_of_date DATE NOT NULL,
  alloc_type ENUM('sector','country','asset_class','other') NOT NULL DEFAULT 'other',
  alloc_name VARCHAR(255) NOT NULL,
  weight_pct DECIMAL(12,6),
  source_url TEXT,
  scraped_at DATETIME NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_th_alloc_fund_date (fund_code, as_of_date),
  KEY idx_th_alloc_type (alloc_type)
);

CREATE TABLE IF NOT EXISTS raw_thai_funds_returns (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  return_1y DECIMAL(12,6),
  return_3y DECIMAL(12,6),
  source_url TEXT,
  scraped_at DATETIME NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_th_return_fund (fund_code)
);

USE raw_ft;

CREATE TABLE IF NOT EXISTS raw_ft_static_detail (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ft_ticker VARCHAR(128) NOT NULL,
  ticker VARCHAR(128),
  fund_name VARCHAR(512),
  ticker_type VARCHAR(128),
  isin_code VARCHAR(64),
  domicile_country VARCHAR(128),
  aum_value DECIMAL(24,2),
  aum_currency VARCHAR(16),
  date_scraper DATE NOT NULL,
  source_url TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ft_static_ticker_date (ft_ticker, date_scraper),
  KEY idx_ft_static_isin (isin_code)
);

CREATE TABLE IF NOT EXISTS raw_ft_holdings (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ft_ticker VARCHAR(128) NOT NULL,
  ticker VARCHAR(128),
  holding_name VARCHAR(512),
  holding_ticker VARCHAR(128),
  holding_type VARCHAR(128),
  allocation_type VARCHAR(128),
  portfolio_weight_pct DECIMAL(12,6),
  date_scraper DATE NOT NULL,
  source_url TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ft_holdings_ticker_date (ticker, date_scraper),
  KEY idx_ft_holdings_symbol (holding_ticker)
);

CREATE TABLE IF NOT EXISTS raw_ft_sector_allocation (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ticker VARCHAR(128) NOT NULL,
  category_name VARCHAR(255) NOT NULL,
  weight_pct DECIMAL(12,6),
  date_scraper DATE NOT NULL,
  source_url TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ft_sector_ticker_date (ticker, date_scraper)
);

CREATE TABLE IF NOT EXISTS raw_ft_country_allocation (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ticker VARCHAR(128) NOT NULL,
  category_name VARCHAR(255) NOT NULL,
  weight_pct DECIMAL(12,6),
  date_scraper DATE NOT NULL,
  source_url TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ft_country_ticker_date (ticker, date_scraper)
);

CREATE TABLE IF NOT EXISTS raw_ft_returns (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ticker VARCHAR(128) NOT NULL,
  return_1y DECIMAL(12,6),
  return_3y DECIMAL(12,6),
  date_scraper DATE NOT NULL,
  source_url TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ft_returns_ticker_date (ticker, date_scraper)
);

-- =========================================================
-- 2) CORE / TRACEABILITY LAYER
-- =========================================================

USE fund_traceability;

CREATE TABLE IF NOT EXISTS map_thai_to_master (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  feeder_name VARCHAR(512),
  feeder_weight_pct DECIMAL(12,6),
  feeder_weight_pct_norm DECIMAL(12,6),
  token VARCHAR(255),
  token_isin VARCHAR(64),
  ft_ticker VARCHAR(128),
  ticker VARCHAR(128),
  map_method ENUM('isin','name','manual','unmapped') NOT NULL DEFAULT 'unmapped',
  as_of_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_map_fund (fund_code),
  KEY idx_map_ticker (ticker),
  KEY idx_map_method (map_method)
);

CREATE TABLE IF NOT EXISTS fact_effective_exposure_stock (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  ft_ticker VARCHAR(128),
  ticker VARCHAR(128),
  map_method VARCHAR(32),
  feeder_name VARCHAR(512),
  feeder_weight_pct DECIMAL(12,6),
  feeder_weight_pct_norm DECIMAL(12,6),
  holding_name VARCHAR(512),
  holding_ticker VARCHAR(128),
  holding_ticker_norm VARCHAR(128),
  holding_type VARCHAR(128),
  portfolio_weight_pct DECIMAL(12,6),
  true_weight_pct DECIMAL(16,8),
  aum_thb DECIMAL(24,2),
  true_value_thb DECIMAL(24,2),
  nav_as_of_date DATE,
  date_scraper DATE,
  holding_key VARCHAR(256),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_fact_stock_fund (fund_code),
  KEY idx_fact_stock_symbol (holding_ticker_norm),
  KEY idx_fact_stock_value (true_value_thb)
);

CREATE TABLE IF NOT EXISTS fact_effective_exposure_sector (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  ticker VARCHAR(128),
  sector_name VARCHAR(255) NOT NULL,
  true_weight_pct DECIMAL(16,8),
  true_value_thb DECIMAL(24,2),
  nav_as_of_date DATE,
  date_scraper DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_fact_sector_fund (fund_code),
  KEY idx_fact_sector_name (sector_name)
);

CREATE TABLE IF NOT EXISTS fact_effective_exposure_country (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  ticker VARCHAR(128),
  country_name VARCHAR(255) NOT NULL,
  true_weight_pct DECIMAL(16,8),
  true_value_thb DECIMAL(24,2),
  nav_as_of_date DATE,
  date_scraper DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_fact_country_fund (fund_code),
  KEY idx_fact_country_name (country_name)
);

-- =========================================================
-- 3) AGGREGATION LAYER
-- =========================================================

CREATE TABLE IF NOT EXISTS agg_top_thai_holdings (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  rank_no INT NOT NULL,
  symbol VARCHAR(128),
  holding_name VARCHAR(512),
  holding_type VARCHAR(128),
  total_thai_fund_value DECIMAL(24,2),
  total_weight_pct DECIMAL(16,8),
  total_funds_holding INT,
  as_of_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_agg_th_rank (rank_no),
  KEY idx_agg_th_symbol (symbol)
);

CREATE TABLE IF NOT EXISTS agg_top_global_traceability (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  rank_no INT NOT NULL,
  holding_key VARCHAR(256),
  holding_ticker VARCHAR(128),
  holding_name VARCHAR(512),
  holding_type VARCHAR(128),
  total_true_weight_pct DECIMAL(16,8),
  global_fund_value DECIMAL(24,2),
  as_of_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_agg_gl_rank (rank_no),
  KEY idx_agg_gl_symbol (holding_ticker)
);

CREATE TABLE IF NOT EXISTS agg_sector_exposure (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  sector_name VARCHAR(255) NOT NULL,
  total_true_weight_pct DECIMAL(16,8),
  total_true_value_thb DECIMAL(24,2),
  allocation_share_pct DECIMAL(16,8),
  scope ENUM('TH','FOREIGN','ALL') NOT NULL DEFAULT 'ALL',
  as_of_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_agg_sector_scope (scope),
  KEY idx_agg_sector_value (total_true_value_thb)
);

CREATE TABLE IF NOT EXISTS agg_country_exposure (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  country_name VARCHAR(255) NOT NULL,
  total_true_weight_pct DECIMAL(16,8),
  total_true_value_thb DECIMAL(24,2),
  allocation_share_pct DECIMAL(16,8),
  scope ENUM('TH','FOREIGN','ALL') NOT NULL DEFAULT 'ALL',
  as_of_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_agg_country_scope (scope),
  KEY idx_agg_country_value (total_true_value_thb)
);

CREATE TABLE IF NOT EXISTS agg_dashboard_cards (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  scope ENUM('TH','FOREIGN','ALL') NOT NULL,
  total_holdings_value_thb DECIMAL(24,2),
  top_sector_name VARCHAR(255),
  top_sector_weight_pct DECIMAL(16,8),
  top_country_name VARCHAR(255),
  top_country_weight_pct DECIMAL(16,8),
  avg_fund_return_1y DECIMAL(16,8),
  avg_fund_return_3y DECIMAL(16,8),
  mapped_fund_count INT,
  mapped_master_count INT,
  as_of_date DATE,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_agg_card_scope (scope)
);

CREATE TABLE IF NOT EXISTS agg_search_asset_to_funds (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ticker_input VARCHAR(128) NOT NULL,
  fund_code VARCHAR(64) NOT NULL,
  fund_name VARCHAR(255),
  investment_method VARCHAR(64),
  holding_value_thb DECIMAL(24,2),
  nav_thb DECIMAL(24,2),
  pct_nav DECIMAL(16,8),
  rank_no INT,
  as_of_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_agg_asset_funds_ticker (ticker_input),
  KEY idx_agg_asset_funds_fund (fund_code)
);

CREATE TABLE IF NOT EXISTS agg_search_fund_to_assets (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_code VARCHAR(64) NOT NULL,
  fund_name VARCHAR(255),
  symbol VARCHAR(128),
  holding_name VARCHAR(512),
  investment_method VARCHAR(64),
  holding_value_thb DECIMAL(24,2),
  nav_thb DECIMAL(24,2),
  pct_nav DECIMAL(16,8),
  rank_no INT,
  as_of_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_agg_fund_assets_fund (fund_code),
  KEY idx_agg_fund_assets_symbol (symbol)
);

-- =========================================================
-- 4) API CONTRACT TABLES
-- =========================================================

USE funds_api;

CREATE TABLE IF NOT EXISTS api_stocks (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(128) NOT NULL UNIQUE,
  full_name VARCHAR(512),
  sector VARCHAR(255),
  stock_type ENUM('TH','FOREIGN','GOLD') DEFAULT 'FOREIGN',
  country VARCHAR(128),
  percent_change DECIMAL(10,4) DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_api_stocks_type (stock_type),
  KEY idx_api_stocks_country (country)
);

CREATE TABLE IF NOT EXISTS api_funds (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(64) NOT NULL UNIQUE,
  name_th VARCHAR(255) NOT NULL,
  name_en VARCHAR(255),
  amc VARCHAR(255),
  category VARCHAR(255),
  risk_level INT,
  return_1y DECIMAL(16,8),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_api_funds_name (name_th)
);

CREATE TABLE IF NOT EXISTS api_stock_aggregates (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  stock_id BIGINT NOT NULL,
  total_exposure_value DECIMAL(24,2),
  portfolio_weight DECIMAL(16,8),
  exposure_type VARCHAR(64),
  total_funds_holding INT,
  total_thai_fund_value DECIMAL(24,2),
  global_fund_value DECIMAL(24,2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_api_stock_agg_stock (stock_id),
  CONSTRAINT fk_api_stock_agg_stock FOREIGN KEY (stock_id) REFERENCES api_stocks(id)
);

CREATE TABLE IF NOT EXISTS api_fund_holdings (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fund_id BIGINT NOT NULL,
  stock_id BIGINT NOT NULL,
  ranking INT,
  investment_method ENUM('Direct','Feeder Fund','Other') DEFAULT 'Other',
  holding_value_thb DECIMAL(24,2),
  nav_thb DECIMAL(24,2),
  pct_nav DECIMAL(16,8),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_api_fh_fund (fund_id),
  KEY idx_api_fh_stock (stock_id),
  CONSTRAINT fk_api_fh_fund FOREIGN KEY (fund_id) REFERENCES api_funds(id),
  CONSTRAINT fk_api_fh_stock FOREIGN KEY (stock_id) REFERENCES api_stocks(id)
);

CREATE TABLE IF NOT EXISTS api_suggestions (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  keyword_text VARCHAR(255) NOT NULL,
  item_type ENUM('STOCK','FUND') NOT NULL,
  item_code VARCHAR(128) NOT NULL,
  item_name VARCHAR(512),
  priority_score INT DEFAULT 100,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_api_suggest_keyword (keyword_text),
  KEY idx_api_suggest_type (item_type)
);

