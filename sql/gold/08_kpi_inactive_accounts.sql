-- KPI 3: Inactive accounts — no transaction activity in last 90 days

CREATE OR REPLACE TABLE `bank_analytics.kpi_inactive_accounts` AS
WITH last_txn AS (
  SELECT
    account_sk,
    MAX(transaction_date) AS last_txn_date
  FROM `bank_analytics.fact_transactions`
  GROUP BY account_sk
)
SELECT
  a.AccountID,
  a.CustomerID,
  a.AccountTypeID,
  a.Status,
  lt.last_txn_date,
  DATE_DIFF(CURRENT_DATE(), lt.last_txn_date, DAY) AS days_inactive,
  CASE
    WHEN lt.last_txn_date IS NULL THEN 'Never Active'
    WHEN DATE_DIFF(CURRENT_DATE(), lt.last_txn_date, DAY) > 180 THEN 'Dormant'
    ELSE 'Inactive'
  END AS inactivity_category
FROM `bank_analytics.dim_accounts`  a
LEFT JOIN last_txn                   lt ON a.surrogate_key = lt.account_sk
WHERE a.is_current = TRUE
  AND a.Status = 'Active'
  AND (lt.last_txn_date IS NULL
       OR DATE_DIFF(CURRENT_DATE(), lt.last_txn_date, DAY) > 90)
ORDER BY days_inactive DESC;
