CREATE DATABASE IF NOT EXISTS fund_traceability;
USE fund_traceability;

CREATE TABLE IF NOT EXISTS daily_fx_rates (
  date_rate DATE NOT NULL,
  from_ccy VARCHAR(10) NOT NULL,
  to_ccy VARCHAR(10) NOT NULL DEFAULT 'THB',
  rate_to_thb DECIMAL(20,8) NOT NULL,
  source_system VARCHAR(50) NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (date_rate, from_ccy, to_ccy),
  KEY idx_fx_from_to_date (from_ccy, to_ccy, date_rate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Backward compatibility alias for old table name.
DROP VIEW IF EXISTS dim_fx_rates;
CREATE VIEW dim_fx_rates AS
SELECT date_rate, from_ccy, to_ccy, rate_to_thb, source_system, updated_at
FROM daily_fx_rates;

-- Example seed rows (replace with real rates)
-- INSERT INTO daily_fx_rates (date_rate, from_ccy, to_ccy, rate_to_thb, source_system)
-- VALUES
--   ('2026-01-01', 'USD', 'THB', 34.12000000, 'manual'),
--   ('2026-01-01', 'EUR', 'THB', 37.55000000, 'manual');
