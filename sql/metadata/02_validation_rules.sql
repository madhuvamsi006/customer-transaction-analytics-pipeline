-- Metadata: Validation rule registry
-- Stores DQ rules that drive Silver-layer checks.
-- Mirrors validation_rules.yaml for SQL-native access.

CREATE TABLE IF NOT EXISTS `bank_analytics.metadata_validation_rules`
(
  rule_id     STRING NOT NULL,
  rule_name   STRING NOT NULL,
  entity      STRING NOT NULL,
  check_type  STRING NOT NULL,
  rule_config STRING,           -- JSON blob of all rule parameters
  action      STRING,           -- flag | quarantine | suppress
  description STRING,
  is_active   BOOL   DEFAULT TRUE,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (description = 'Data quality rule registry');

INSERT INTO `bank_analytics.metadata_validation_rules`
  (rule_id, rule_name, entity, check_type, rule_config, action, description)
VALUES
  ('VR001','duplicate_customer','Customers','deduplication',
   '{"columns":["CustomerID"]}','quarantine',
   'Flag and quarantine exact duplicate CustomerID records'),
  ('VR002','missing_email','Customers','not_null',
   '{"columns":["Email"]}','flag',
   'Flag customers with missing email address'),
  ('VR003','missing_phone','Customers','not_null',
   '{"columns":["Phone"]}','flag',
   'Flag customers with missing phone number'),
  ('VR004','duplicate_transaction','Transactions','deduplication',
   '{"columns":["TransactionID","AccountID","Amount","TransactionDate","ReferenceNo"]}','suppress',
   'Suppress exact duplicate transactions'),
  ('VR005','currency_mismatch','Transactions','referential',
   '{"ref_entity":"Accounts","ref_column":"Currency","join_column":"AccountID","compare_column":"Currency"}','flag',
   'Flag transactions where currency differs from account currency'),
  ('VR006','invalid_transaction_date','Transactions','date_range',
   '{"date_column":"TransactionDate","ref_entity":"Accounts","ref_open_col":"OpenDate","ref_close_col":"CloseDate"}','flag',
   'Flag transactions outside account open/close date range'),
  ('VR007','sortcode_integrity','Accounts','prefix_match',
   '{"column":"SortCode","ref_entity":"Branches","ref_column":"SortCode","join_column":"BranchID"}','flag',
   'Validate account sort code matches branch sort code'),
  ('VR008','negative_balance_overdraft','Accounts','threshold',
   '{"column":"Balance","ref_entity":"AccountTypes","ref_column":"OverdraftLimit","join_column":"AccountTypeID"}','flag',
   'Flag accounts where negative balance exceeds overdraft limit'),
  ('VR009','high_value_transaction','Transactions','threshold',
   '{"column":"Amount","threshold_value":100000}','flag',
   'Flag transactions exceeding high-value threshold of 100000'),
  ('VR010','structuring_detection','Transactions','structuring',
   '{"column":"Amount","near_threshold":100000,"tolerance":0.05,"window_days":3,"min_count":2,"channel_filter":["Branch","ATM"]}','flag',
   'Flag repeated near-threshold deposits via Branch/ATM in short window'),
  ('VR011','dormant_reactivation','Transactions','dormant_burst',
   '{"inactivity_days":90,"burst_window_days":7,"burst_min_transactions":3}','flag',
   'Flag accounts with long inactivity followed by clustered transactions');
