-- KPI 4: Suspicious transactions — high-value (> ₹100,000) and structuring flags

CREATE OR REPLACE TABLE `bank_analytics.kpi_suspicious_transactions` AS
SELECT
  f.transaction_id,
  f.transaction_date,
  f.amount,
  f.currency,
  f.channel,
  f.description,
  f.status,
  a.AccountID,
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)    AS customer_name,
  b.BranchID,
  b.BranchName,
  CASE
    WHEN f.amount > 100000 THEN 'High Value (> 100K)'
    WHEN f.amount BETWEEN 95000 AND 100000
      AND f.channel IN ('Branch', 'ATM') THEN 'Structuring Suspect'
    ELSE 'Review Required'
  END                                      AS flag_reason
FROM `bank_analytics.fact_transactions`  f
LEFT JOIN `bank_analytics.dim_accounts`   a ON f.account_sk  = a.surrogate_key AND a.is_current
LEFT JOIN `bank_analytics.dim_customers`  c ON f.customer_sk = c.surrogate_key AND c.is_current
LEFT JOIN `bank_analytics.dim_branches`   b ON f.branch_sk   = b.surrogate_key
WHERE f.amount > 95000
ORDER BY f.amount DESC;
