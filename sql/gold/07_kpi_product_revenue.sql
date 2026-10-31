-- KPI 2: Product-wise (account type) revenue contribution

CREATE OR REPLACE TABLE `bank_analytics.kpi_product_revenue` AS
SELECT
  p.AccountTypeID,
  p.AccountTypeName,
  SUM(f.amount)                                           AS total_amount,
  COUNT(f.transaction_id)                                 AS txn_count,
  ROUND(SUM(f.amount) / SUM(SUM(f.amount)) OVER () * 100, 2) AS revenue_pct
FROM `bank_analytics.fact_transactions`  f
JOIN `bank_analytics.dim_accounttypes`   p ON f.product_sk = p.surrogate_key
GROUP BY p.AccountTypeID, p.AccountTypeName
ORDER BY total_amount DESC;
