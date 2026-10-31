-- Silver layer table DDL for BigQuery
-- Typed, cleansed, deduplicated records with Silver metadata column.

CREATE TABLE IF NOT EXISTS `bank_analytics.silver_customers`
(
  CustomerID           INT64     NOT NULL,
  FirstName            STRING,
  LastName             STRING,
  Email                STRING,
  Phone                STRING,
  Address              STRING,
  City                 STRING,
  State                STRING,
  Country              STRING,
  PostalCode           STRING,
  DateOfBirth          DATE,
  JoinDate             DATE,
  CustomerType         STRING,
  Status               STRING,
  _source_file         STRING,
  _batch_id            STRING,
  _load_timestamp      STRING,
  _row_hash            STRING,
  _silver_load_timestamp TIMESTAMP
)
PARTITION BY DATE(_silver_load_timestamp)
CLUSTER BY CustomerID
OPTIONS (description = 'Silver: cleansed and deduplicated Customers');

CREATE TABLE IF NOT EXISTS `bank_analytics.silver_accounts`
(
  AccountID            INT64     NOT NULL,
  CustomerID           INT64,
  AccountTypeID        STRING,
  BranchID             STRING,
  SortCode             STRING,
  AccountNumber        STRING,
  OpenDate             DATE,
  CloseDate            DATE,
  Balance              FLOAT64,
  Currency             STRING,
  Status               STRING,
  _source_file         STRING,
  _batch_id            STRING,
  _load_timestamp      STRING,
  _row_hash            STRING,
  _silver_load_timestamp TIMESTAMP
)
PARTITION BY DATE(_silver_load_timestamp)
CLUSTER BY AccountID, CustomerID
OPTIONS (description = 'Silver: cleansed and deduplicated Accounts');

CREATE TABLE IF NOT EXISTS `bank_analytics.silver_transactions`
(
  TransactionID        STRING    NOT NULL,
  AccountID            INT64,
  TransactionTypeID    STRING,
  TransactionDate      DATE,
  Amount               FLOAT64,
  Currency             STRING,
  Description          STRING,
  Channel              STRING,
  ReferenceNo          STRING,
  Status               STRING,
  _source_file         STRING,
  _batch_id            STRING,
  _load_timestamp      STRING,
  _row_hash            STRING,
  _silver_load_timestamp TIMESTAMP
)
PARTITION BY TransactionDate
CLUSTER BY AccountID, TransactionTypeID
OPTIONS (description = 'Silver: cleansed and deduplicated Transactions');

CREATE TABLE IF NOT EXISTS `bank_analytics.silver_branches`
(
  BranchID   STRING NOT NULL,
  BranchName STRING,
  SortCode   STRING,
  Address    STRING,
  City       STRING,
  State      STRING,
  ManagerID  INT64,
  Phone      STRING,
  _silver_load_timestamp TIMESTAMP
)
OPTIONS (description = 'Silver: Branches reference data');

CREATE TABLE IF NOT EXISTS `bank_analytics.silver_accounttypes`
(
  AccountTypeID   STRING NOT NULL,
  AccountTypeName STRING,
  InterestRate    FLOAT64,
  OverdraftLimit  FLOAT64,
  MinBalance      FLOAT64,
  Description     STRING,
  _silver_load_timestamp TIMESTAMP
)
OPTIONS (description = 'Silver: AccountTypes reference data');

CREATE TABLE IF NOT EXISTS `bank_analytics.silver_loans`
(
  LoanID       STRING NOT NULL,
  CustomerID   INT64,
  AccountID    INT64,
  LoanType     STRING,
  Principal    FLOAT64,
  InterestRate FLOAT64,
  StartDate    DATE,
  EndDate      DATE,
  EMIAmount    FLOAT64,
  Currency     STRING,
  Status       STRING,
  _silver_load_timestamp TIMESTAMP
)
OPTIONS (description = 'Silver: Loans data');

CREATE TABLE IF NOT EXISTS `bank_analytics.silver_loanpayments`
(
  PaymentID   STRING NOT NULL,
  LoanID      STRING,
  PaymentDate DATE,
  Amount      FLOAT64,
  Currency    STRING,
  Status      STRING,
  Remarks     STRING,
  _silver_load_timestamp TIMESTAMP
)
PARTITION BY PaymentDate
OPTIONS (description = 'Silver: LoanPayments data');

CREATE TABLE IF NOT EXISTS `bank_analytics.silver_cards`
(
  CardID     STRING NOT NULL,
  AccountID  INT64,
  CustomerID INT64,
  CardType   STRING,
  CardNumber STRING,
  ExpiryDate DATE,
  Status     STRING,
  IssueDate  DATE,
  DailyLimit FLOAT64,
  _silver_load_timestamp TIMESTAMP
)
OPTIONS (description = 'Silver: Cards data');
