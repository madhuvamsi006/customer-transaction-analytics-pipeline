-- Gold: dim_accounttypes / product dimension (SCD Type 1)

CREATE TABLE IF NOT EXISTS `bank_analytics.dim_accounttypes`
(
  surrogate_key   STRING NOT NULL,
  AccountTypeID   STRING NOT NULL,
  AccountTypeName STRING,
  InterestRate    FLOAT64,
  OverdraftLimit  FLOAT64,
  MinBalance      FLOAT64,
  Description     STRING
)
OPTIONS (description = 'Gold dimension: Account Types / Products');
