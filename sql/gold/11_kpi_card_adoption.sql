-- KPI 7: Card product adoption and cross-sell candidates

CREATE OR REPLACE TABLE `bank_analytics.kpi_card_adoption` AS
-- Customers with at least one card
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName) AS customer_name,
  c.CustomerType,
  cd.CardID,
  cd.CardType,
  cd.Status                             AS card_status,
  cd.ExpiryDate,
  cd.DailyLimit,
  'Has Card'                            AS recommendation
FROM `bank_analytics.dim_customers`  c
JOIN `bank_analytics.silver_cards`   cd ON c.CustomerID = cd.CustomerID
WHERE c.is_current = TRUE

UNION ALL

-- Active customers without any card — cross-sell candidates
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName) AS customer_name,
  c.CustomerType,
  NULL                                  AS CardID,
  NULL                                  AS CardType,
  NULL                                  AS card_status,
  NULL                                  AS ExpiryDate,
  NULL                                  AS DailyLimit,
  'Cross-sell Candidate'                AS recommendation
FROM `bank_analytics.dim_customers` c
WHERE c.is_current = TRUE
  AND c.Status = 'Active'
  AND c.CustomerID NOT IN (
    SELECT DISTINCT CustomerID FROM `bank_analytics.silver_cards`
  )
ORDER BY CustomerID;
