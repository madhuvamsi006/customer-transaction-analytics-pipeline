-- Gold: dim_branches (SCD Type 1 — overwrite on change)

CREATE TABLE IF NOT EXISTS `bank_analytics.dim_branches`
(
  surrogate_key STRING NOT NULL,
  BranchID      STRING NOT NULL,
  BranchName    STRING,
  SortCode      STRING,
  Address       STRING,
  City          STRING,
  State         STRING,
  ManagerID     INT64,
  Phone         STRING
)
OPTIONS (description = 'Gold dimension: Branches');
