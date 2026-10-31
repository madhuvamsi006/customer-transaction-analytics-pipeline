-- Metadata: Entity registry table
-- Stores configuration for each entity managed by the pipeline.
-- This mirrors entities.yaml and can be queried at runtime.

CREATE TABLE IF NOT EXISTS `bank_analytics.metadata_entity_registry`
(
  entity_name        STRING    NOT NULL,
  primary_key        STRING    NOT NULL,
  required_fields    STRING,           -- JSON array of required field names
  scd_type           INTEGER,
  scd_track_columns  STRING,           -- JSON array of tracked columns
  target_table       STRING,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (description = 'Metadata registry: entity definitions driving Bronze/Silver/Gold pipeline');

-- Seed data
INSERT INTO `bank_analytics.metadata_entity_registry`
  (entity_name, primary_key, required_fields, scd_type, scd_track_columns, target_table)
VALUES
  ('Customers',        'CustomerID',       '["CustomerID","FirstName","LastName","Email","Phone"]',
   2, '["CustomerType","Status","Address","City","State","Email","Phone"]', 'customers'),
  ('Accounts',         'AccountID',        '["AccountID","CustomerID","AccountTypeID","BranchID","SortCode","AccountNumber","OpenDate","Balance","Currency"]',
   2, '["Balance","Status","CloseDate"]', 'accounts'),
  ('Transactions',     'TransactionID',    '["TransactionID","AccountID","TransactionTypeID","TransactionDate","Amount","Currency"]',
   1, NULL, 'transactions'),
  ('Branches',         'BranchID',         '["BranchID","BranchName","SortCode"]',
   1, NULL, 'branches'),
  ('AccountTypes',     'AccountTypeID',    '["AccountTypeID","AccountTypeName","OverdraftLimit"]',
   1, NULL, 'account_types'),
  ('TransactionTypes', 'TransactionTypeID','["TransactionTypeID","TransactionTypeName","Direction"]',
   1, NULL, 'transaction_types'),
  ('Loans',            'LoanID',           '["LoanID","CustomerID","AccountID","LoanType","Principal","InterestRate","StartDate","EMIAmount"]',
   1, NULL, 'loans'),
  ('LoanPayments',     'PaymentID',        '["PaymentID","LoanID","PaymentDate","Amount","Status"]',
   1, NULL, 'loan_payments'),
  ('Cards',            'CardID',           '["CardID","AccountID","CustomerID","CardType","CardNumber","ExpiryDate","Status"]',
   1, NULL, 'cards');
