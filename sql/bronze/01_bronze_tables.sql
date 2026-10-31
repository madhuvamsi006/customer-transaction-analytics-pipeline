-- Bronze layer table DDL for BigQuery
-- All columns are STRING; type casting happens in Silver.
-- Tables are partitioned by _load_timestamp for efficient replay queries.

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_customers`
(
  CustomerID    STRING,
  FirstName     STRING,
  LastName      STRING,
  Email         STRING,
  Phone         STRING,
  Address       STRING,
  City          STRING,
  State         STRING,
  Country       STRING,
  PostalCode    STRING,
  DateOfBirth   STRING,
  JoinDate      STRING,
  CustomerType  STRING,
  Status        STRING,
  _source_file      STRING,
  _batch_id         STRING,
  _load_timestamp   STRING,
  _entity_name      STRING,
  _row_hash         STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw Customers records');

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_accounts`
(
  AccountID     STRING,
  CustomerID    STRING,
  AccountTypeID STRING,
  BranchID      STRING,
  SortCode      STRING,
  AccountNumber STRING,
  OpenDate      STRING,
  CloseDate     STRING,
  Balance       STRING,
  Currency      STRING,
  Status        STRING,
  _source_file      STRING,
  _batch_id         STRING,
  _load_timestamp   STRING,
  _entity_name      STRING,
  _row_hash         STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw Accounts records');

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_transactions`
(
  TransactionID     STRING,
  AccountID         STRING,
  TransactionTypeID STRING,
  TransactionDate   STRING,
  Amount            STRING,
  Currency          STRING,
  Description       STRING,
  Channel           STRING,
  ReferenceNo       STRING,
  Status            STRING,
  _source_file      STRING,
  _batch_id         STRING,
  _load_timestamp   STRING,
  _entity_name      STRING,
  _row_hash         STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw Transactions records');

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_branches`
(
  BranchID   STRING,
  BranchName STRING,
  SortCode   STRING,
  Address    STRING,
  City       STRING,
  State      STRING,
  ManagerID  STRING,
  Phone      STRING,
  _source_file    STRING,
  _batch_id       STRING,
  _load_timestamp STRING,
  _entity_name    STRING,
  _row_hash       STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw Branches records');

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_accounttypes`
(
  AccountTypeID   STRING,
  AccountTypeName STRING,
  InterestRate    STRING,
  OverdraftLimit  STRING,
  MinBalance      STRING,
  Description     STRING,
  _source_file    STRING,
  _batch_id       STRING,
  _load_timestamp STRING,
  _entity_name    STRING,
  _row_hash       STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw AccountTypes records');

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_transactiontypes`
(
  TransactionTypeID   STRING,
  TransactionTypeName STRING,
  Description         STRING,
  Direction           STRING,
  _source_file        STRING,
  _batch_id           STRING,
  _load_timestamp     STRING,
  _entity_name        STRING,
  _row_hash           STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw TransactionTypes records');

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_loans`
(
  LoanID       STRING,
  CustomerID   STRING,
  AccountID    STRING,
  LoanType     STRING,
  Principal    STRING,
  InterestRate STRING,
  StartDate    STRING,
  EndDate      STRING,
  EMIAmount    STRING,
  Currency     STRING,
  Status       STRING,
  _source_file    STRING,
  _batch_id       STRING,
  _load_timestamp STRING,
  _entity_name    STRING,
  _row_hash       STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw Loans records');

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_loanpayments`
(
  PaymentID   STRING,
  LoanID      STRING,
  PaymentDate STRING,
  Amount      STRING,
  Currency    STRING,
  Status      STRING,
  Remarks     STRING,
  _source_file    STRING,
  _batch_id       STRING,
  _load_timestamp STRING,
  _entity_name    STRING,
  _row_hash       STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw LoanPayments records');

CREATE TABLE IF NOT EXISTS `bank_analytics.bronze_cards`
(
  CardID     STRING,
  AccountID  STRING,
  CustomerID STRING,
  CardType   STRING,
  CardNumber STRING,
  ExpiryDate STRING,
  Status     STRING,
  IssueDate  STRING,
  DailyLimit STRING,
  _source_file    STRING,
  _batch_id       STRING,
  _load_timestamp STRING,
  _entity_name    STRING,
  _row_hash       STRING
)
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (description = 'Bronze: raw Cards records');
