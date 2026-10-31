-- KPI 6: Loan portfolio & repayment health

CREATE OR REPLACE TABLE `bank_analytics.kpi_loan_portfolio_health` AS
WITH payment_stats AS (
  SELECT
    LoanID,
    SUM(CASE WHEN Status = 'Completed' THEN Amount ELSE 0 END) AS total_paid,
    SUM(CASE WHEN Status = 'Failed'    THEN Amount ELSE 0 END) AS total_failed,
    COUNT(*)                                                     AS payment_count,
    SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END)         AS failed_count
  FROM `bank_analytics.silver_loanpayments`
  GROUP BY LoanID
)
SELECT
  l.LoanID,
  l.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)        AS customer_name,
  l.LoanType,
  l.Principal,
  l.EMIAmount,
  l.Status                                     AS loan_status,
  l.StartDate,
  l.EndDate,
  ps.total_paid,
  ps.total_failed,
  ps.payment_count,
  ps.failed_count,
  CASE
    WHEN ps.failed_count = 0                     THEN 'Healthy'
    WHEN ps.failed_count BETWEEN 1 AND 2         THEN 'Watch'
    ELSE                                              'At Risk'
  END                                           AS repayment_health,
  ROUND((l.Principal - COALESCE(ps.total_paid, 0)) / l.Principal * 100, 2)
                                                AS outstanding_pct
FROM `bank_analytics.silver_loans`      l
LEFT JOIN payment_stats                  ps ON l.LoanID = ps.LoanID
LEFT JOIN `bank_analytics.dim_customers` c  ON l.CustomerID = c.CustomerID AND c.is_current
ORDER BY ps.failed_count DESC NULLS LAST;
