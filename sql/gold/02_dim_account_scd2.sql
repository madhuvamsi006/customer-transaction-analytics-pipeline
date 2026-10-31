-- Gold: dim_accounts (SCD Type 2)
-- Tracks Balance, Status, and CloseDate changes across Day 0 → 1 → 2.

CREATE TABLE IF NOT EXISTS `bank_analytics.dim_accounts`
(
  surrogate_key  STRING  NOT NULL,
  AccountID      INT64   NOT NULL,
  CustomerID     INT64,
  AccountTypeID  STRING,
  BranchID       STRING,
  SortCode       STRING,
  AccountNumber  STRING,
  OpenDate       DATE,
  CloseDate      DATE,
  Balance        FLOAT64,
  Currency       STRING,
  Status         STRING,
  eff_start_date DATE    NOT NULL,
  eff_end_date   DATE,
  is_current     BOOL    NOT NULL
)
CLUSTER BY AccountID, CustomerID, is_current
OPTIONS (description = 'Gold dimension: Account SCD Type 2');

CREATE OR REPLACE VIEW `bank_analytics.v_current_accounts` AS
SELECT * EXCEPT(surrogate_key, eff_start_date, eff_end_date, is_current)
FROM `bank_analytics.dim_accounts`
WHERE is_current = TRUE;

CREATE OR REPLACE VIEW `bank_analytics.v_account_history` AS
SELECT
  AccountID, CustomerID, AccountTypeID, BranchID,
  Balance, Status, CloseDate,
  eff_start_date, eff_end_date, is_current
FROM `bank_analytics.dim_accounts`
ORDER BY AccountID, eff_start_date;
