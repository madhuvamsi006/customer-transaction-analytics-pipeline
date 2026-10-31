-- Quarantine table: rows that failed DQ rules
-- Retains all original columns plus DQ failure metadata.

CREATE TABLE IF NOT EXISTS `bank_analytics.quarantine_all_entities`
(
  -- Entity identity
  _entity_name    STRING,
  _batch_id       STRING,
  _source_file    STRING,
  _row_hash       STRING,
  _load_timestamp STRING,

  -- DQ failure metadata
  _dq_rule_id       STRING NOT NULL,
  _dq_rule_name     STRING,
  _dq_failure_reason STRING,
  _quarantine_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  -- Raw data payload (all columns as string)
  raw_data          JSON
)
PARTITION BY DATE(_quarantine_ts)
CLUSTER BY _entity_name, _dq_rule_id
OPTIONS (description = 'DQ quarantine: failed rows with failure reason and rule reference');

-- Per-entity quarantine views for easier querying
CREATE OR REPLACE VIEW `bank_analytics.quarantine_customers` AS
SELECT * FROM `bank_analytics.quarantine_all_entities`
WHERE _entity_name = 'Customers';

CREATE OR REPLACE VIEW `bank_analytics.quarantine_transactions` AS
SELECT * FROM `bank_analytics.quarantine_all_entities`
WHERE _entity_name = 'Transactions';

CREATE OR REPLACE VIEW `bank_analytics.quarantine_accounts` AS
SELECT * FROM `bank_analytics.quarantine_all_entities`
WHERE _entity_name = 'Accounts';

-- DQ summary: failures per rule per batch
CREATE OR REPLACE VIEW `bank_analytics.v_dq_summary` AS
SELECT
  _entity_name,
  _dq_rule_id,
  _dq_rule_name,
  _batch_id,
  COUNT(*) AS failed_row_count,
  DATE(_quarantine_ts) AS failure_date
FROM `bank_analytics.quarantine_all_entities`
GROUP BY 1, 2, 3, 4, 6
ORDER BY 6 DESC, failed_row_count DESC;
