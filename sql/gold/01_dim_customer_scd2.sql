-- Gold: dim_customers (SCD Type 2)
-- One row per version of a customer record.
-- is_current=TRUE identifies the live/latest version.
-- Historical versions are queryable via eff_start_date / eff_end_date.

CREATE TABLE IF NOT EXISTS `bank_analytics.dim_customers`
(
  surrogate_key  STRING    NOT NULL,   -- SHA-256(CustomerID|eff_start_date)[:16]
  CustomerID     INT64     NOT NULL,
  FirstName      STRING,
  LastName       STRING,
  Email          STRING,
  Phone          STRING,
  Address        STRING,
  City           STRING,
  State          STRING,
  Country        STRING,
  PostalCode     STRING,
  DateOfBirth    DATE,
  JoinDate       DATE,
  CustomerType   STRING,
  Status         STRING,
  eff_start_date DATE      NOT NULL,
  eff_end_date   DATE,               -- NULL when is_current = TRUE
  is_current     BOOL      NOT NULL
)
CLUSTER BY CustomerID, is_current
OPTIONS (description = 'Gold dimension: Customer SCD Type 2');

-- Convenience view: current customers only
CREATE OR REPLACE VIEW `bank_analytics.v_current_customers` AS
SELECT * EXCEPT(surrogate_key, eff_start_date, eff_end_date, is_current)
FROM `bank_analytics.dim_customers`
WHERE is_current = TRUE;

-- Convenience view: full SCD-2 history
CREATE OR REPLACE VIEW `bank_analytics.v_customer_history` AS
SELECT
  CustomerID,
  FirstName, LastName, Email, CustomerType, Status,
  eff_start_date,
  eff_end_date,
  is_current,
  DATE_DIFF(
    COALESCE(eff_end_date, CURRENT_DATE()),
    eff_start_date,
    DAY
  ) AS days_in_version
FROM `bank_analytics.dim_customers`
ORDER BY CustomerID, eff_start_date;
