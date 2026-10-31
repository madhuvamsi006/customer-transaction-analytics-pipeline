-- Gold: fact_transactions
-- Grain: one row per transaction (TransactionID).
-- Surrogate keys from all relevant dimensions.

CREATE TABLE IF NOT EXISTS `bank_analytics.fact_transactions`
(
  transaction_id      STRING    NOT NULL,
  account_sk          STRING,
  customer_sk         STRING,
  branch_sk           STRING,
  product_sk          STRING,
  txn_type_sk         STRING,
  transaction_date    DATE,
  amount              FLOAT64,
  currency            STRING,
  channel             STRING,
  description         STRING,
  status              STRING,
  reference_no        STRING,
  _gold_load_timestamp TIMESTAMP
)
PARTITION BY transaction_date
CLUSTER BY account_sk, customer_sk
OPTIONS (description = 'Gold fact: Transactions star schema');

-- Conformed measure view joining all dimensions
CREATE OR REPLACE VIEW `bank_analytics.v_fact_transactions_enriched` AS
SELECT
  f.transaction_id,
  f.transaction_date,
  f.amount,
  f.currency,
  f.channel,
  f.status,
  f.description,
  f.reference_no,
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)   AS customer_name,
  c.CustomerType,
  a.AccountID,
  a.AccountNumber,
  a.AccountTypeID,
  b.BranchID,
  b.BranchName,
  b.City                                  AS branch_city,
  p.AccountTypeName                       AS product_name,
  tt.TransactionTypeName                  AS transaction_type,
  tt.Direction
FROM `bank_analytics.fact_transactions`     f
LEFT JOIN `bank_analytics.dim_customers`    c ON f.customer_sk  = c.surrogate_key AND c.is_current
LEFT JOIN `bank_analytics.dim_accounts`     a ON f.account_sk   = a.surrogate_key AND a.is_current
LEFT JOIN `bank_analytics.dim_branches`     b ON f.branch_sk    = b.surrogate_key
LEFT JOIN `bank_analytics.dim_accounttypes` p ON f.product_sk   = p.surrogate_key
LEFT JOIN `bank_analytics.dim_transactiontypes` tt ON f.txn_type_sk = tt.surrogate_key;
