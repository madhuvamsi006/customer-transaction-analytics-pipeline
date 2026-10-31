-- KPI 1: Top 5 customers by total transaction volume

CREATE OR REPLACE TABLE `bank_analytics.kpi_top5_customers` AS
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName) AS customer_name,
  c.CustomerType,
  SUM(f.amount)                         AS total_transaction_amount,
  COUNT(f.transaction_id)               AS transaction_count,
  RANK() OVER (ORDER BY SUM(f.amount) DESC) AS volume_rank
FROM `bank_analytics.fact_transactions`  f
JOIN `bank_analytics.dim_customers`      c
  ON f.customer_sk = c.surrogate_key AND c.is_current
GROUP BY c.CustomerID, c.FirstName, c.LastName, c.CustomerType
QUALIFY RANK() OVER (ORDER BY SUM(f.amount) DESC) <= 5
ORDER BY total_transaction_amount DESC;
