"""
Gold layer — reusable KPI outputs.

All KPIs are derived from the Gold fact and dimension tables.
Each method returns a Spark DataFrame and optionally writes to BigQuery.

KPIs implemented
----------------
1. top_5_customers_by_volume         — Top 5 by total transaction amount
2. product_revenue_contribution      — Total txn value by account type
3. inactive_accounts_90days          — Accounts with no txn in last 90 days
4. suspicious_transactions           — Txns exceeding ₹100,000 threshold
5. customer_scd2_history             — Full Customer SCD-2 version history
6. loan_portfolio_health             — Branch/customer loan & repayment risk
7. card_adoption                     — Active cards; cross-sell candidates
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class KPIBuilder:
    """
    Builds and persists Gold KPI tables from fact/dimension DataFrames.

    All KPI methods accept pre-loaded DataFrames so they can be tested
    without a live BigQuery connection.
    """

    def __init__(
        self,
        spark: SparkSession,
        bq_dataset: str,
        gcs_temp_bucket: str,
    ):
        self.spark = spark
        self.bq_dataset = bq_dataset
        self.gcs_temp_bucket = gcs_temp_bucket

    # ------------------------------------------------------------------
    # KPI 1 — Top 5 customers by total transaction volume
    # ------------------------------------------------------------------

    def top_5_customers_by_volume(
        self,
        fact_df: DataFrame,
        dim_customer_df: DataFrame,
    ) -> DataFrame:
        current_cust = dim_customer_df.filter(F.col("is_current") == True)
        result = (
            fact_df
            .groupBy("customer_sk")
            .agg(
                F.sum("amount").alias("total_transaction_amount"),
                F.count("transaction_id").alias("transaction_count"),
            )
            .join(
                current_cust.select(
                    "surrogate_key",
                    F.concat_ws(" ", "FirstName", "LastName").alias("customer_name"),
                    "CustomerID", "CustomerType",
                ).withColumnRenamed("surrogate_key", "customer_sk"),
                on="customer_sk",
                how="left",
            )
            .orderBy(F.col("total_transaction_amount").desc())
            .limit(5)
            .select(
                "CustomerID", "customer_name", "CustomerType",
                "total_transaction_amount", "transaction_count",
            )
        )
        return result

    # ------------------------------------------------------------------
    # KPI 2 — Product-wise revenue contribution
    # ------------------------------------------------------------------

    def product_revenue_contribution(
        self,
        fact_df: DataFrame,
        dim_product_df: DataFrame,
    ) -> DataFrame:
        total = fact_df.agg(F.sum("amount").alias("grand_total")).collect()[0]["grand_total"] or 1.0

        result = (
            fact_df
            .groupBy("product_sk")
            .agg(
                F.sum("amount").alias("total_amount"),
                F.count("transaction_id").alias("txn_count"),
            )
            .join(
                dim_product_df.select(
                    F.col("surrogate_key").alias("product_sk"),
                    "AccountTypeID", "AccountTypeName",
                ),
                on="product_sk",
                how="left",
            )
            .withColumn(
                "revenue_pct",
                F.round(F.col("total_amount") / F.lit(total) * 100, 2),
            )
            .orderBy(F.col("total_amount").desc())
            .select("AccountTypeID", "AccountTypeName", "total_amount", "txn_count", "revenue_pct")
        )
        return result

    # ------------------------------------------------------------------
    # KPI 3 — Inactive accounts (no transaction in last 90 days)
    # ------------------------------------------------------------------

    def inactive_accounts_90days(
        self,
        fact_df: DataFrame,
        dim_account_df: DataFrame,
        reference_date: Optional[str] = None,
    ) -> DataFrame:
        cutoff = (
            date.fromisoformat(reference_date) - timedelta(days=90)
            if reference_date
            else date.today() - timedelta(days=90)
        )

        last_txn = (
            fact_df
            .groupBy("account_sk")
            .agg(F.max("transaction_date").alias("last_txn_date"))
        )

        current_accounts = dim_account_df.filter(
            (F.col("is_current") == True) & (F.col("Status") == "Active")
        )

        result = (
            current_accounts.alias("a")
            .join(
                last_txn.withColumnRenamed("account_sk", "surrogate_key"),
                on="surrogate_key",
                how="left",
            )
            .filter(
                F.col("last_txn_date").isNull()
                | (F.col("last_txn_date") < F.lit(cutoff.isoformat()).cast("date"))
            )
            .select(
                F.col("a.AccountID"),
                F.col("a.CustomerID"),
                F.col("a.AccountTypeID"),
                F.col("a.Status"),
                F.col("last_txn_date"),
                F.datediff(F.current_date(), F.col("last_txn_date")).alias("days_inactive"),
            )
        )
        return result

    # ------------------------------------------------------------------
    # KPI 4 — Suspicious / high-value transactions (> ₹100,000)
    # ------------------------------------------------------------------

    def suspicious_transactions(
        self,
        fact_df: DataFrame,
        dim_customer_df: DataFrame,
        dim_account_df: DataFrame,
        threshold: float = 100_000.0,
    ) -> DataFrame:
        current_cust = dim_customer_df.filter(F.col("is_current") == True)
        current_acc = dim_account_df.filter(F.col("is_current") == True)

        result = (
            fact_df.filter(F.col("amount") > threshold)
            .join(
                current_acc.select(
                    F.col("surrogate_key").alias("account_sk"),
                    "AccountID", "CustomerID",
                ),
                on="account_sk", how="left",
            )
            .join(
                current_cust.select(
                    F.col("surrogate_key").alias("customer_sk"),
                    F.concat_ws(" ", "FirstName", "LastName").alias("customer_name"),
                ),
                on="customer_sk", how="left",
            )
            .select(
                "transaction_id", "transaction_date", "amount", "currency",
                "channel", "description", "status", "AccountID", "customer_name",
            )
            .orderBy(F.col("amount").desc())
        )
        return result

    # ------------------------------------------------------------------
    # KPI 5 — Customer SCD-2 history
    # ------------------------------------------------------------------

    def customer_scd2_history(self, dim_customer_df: DataFrame) -> DataFrame:
        return dim_customer_df.select(
            "CustomerID", "FirstName", "LastName", "Email", "CustomerType", "Status",
            "eff_start_date", "eff_end_date", "is_current", "surrogate_key",
        ).orderBy("CustomerID", "eff_start_date")

    # ------------------------------------------------------------------
    # KPI 6 — Loan portfolio & repayment health
    # ------------------------------------------------------------------

    def loan_portfolio_health(
        self,
        loans_df: DataFrame,
        loan_payments_df: DataFrame,
        dim_customer_df: DataFrame,
        dim_branch_df: DataFrame,
        dim_account_df: DataFrame,
    ) -> DataFrame:
        current_cust = dim_customer_df.filter(F.col("is_current") == True)

        payment_stats = loan_payments_df.groupBy("LoanID").agg(
            F.sum(
                F.when(F.col("Status") == "Completed", F.col("Amount")).otherwise(0)
            ).alias("total_paid"),
            F.sum(
                F.when(F.col("Status") == "Failed", F.col("Amount")).otherwise(0)
            ).alias("total_failed"),
            F.count("*").alias("payment_count"),
            F.sum(
                F.when(F.col("Status") == "Failed", 1).otherwise(0)
            ).alias("failed_count"),
        )

        result = (
            loans_df.alias("l")
            .join(payment_stats, on="LoanID", how="left")
            .join(
                current_cust.select(
                    "CustomerID",
                    F.concat_ws(" ", "FirstName", "LastName").alias("customer_name"),
                ),
                on="CustomerID", how="left",
            )
            .select(
                F.col("l.LoanID"),
                F.col("l.CustomerID"),
                F.col("customer_name"),
                F.col("l.LoanType"),
                F.col("l.Principal"),
                F.col("l.EMIAmount"),
                F.col("l.Status").alias("loan_status"),
                F.col("total_paid"),
                F.col("total_failed"),
                F.col("payment_count"),
                F.col("failed_count"),
                F.when(F.col("failed_count") > 0, "At Risk").otherwise("Healthy").alias("repayment_health"),
            )
            .orderBy(F.col("failed_count").desc())
        )
        return result

    # ------------------------------------------------------------------
    # KPI 7 — Card product adoption
    # ------------------------------------------------------------------

    def card_adoption(
        self,
        cards_df: DataFrame,
        dim_customer_df: DataFrame,
    ) -> DataFrame:
        current_cust = dim_customer_df.filter(F.col("is_current") == True)

        adopted = cards_df.select("CustomerID").distinct()
        cross_sell_candidates = (
            current_cust.join(adopted, on="CustomerID", how="left_anti")
            .filter(F.col("Status") == "Active")
            .withColumn("recommendation", F.lit("Cross-sell candidate"))
        )

        adoption_summary = (
            cards_df.alias("cd")
            .join(
                current_cust.select(
                    "CustomerID",
                    F.concat_ws(" ", "FirstName", "LastName").alias("customer_name"),
                    "CustomerType",
                ),
                on="CustomerID", how="left",
            )
            .select(
                "CustomerID", "customer_name", "CustomerType",
                F.col("cd.CardID"), F.col("cd.CardType"), F.col("cd.Status").alias("card_status"),
                F.col("cd.ExpiryDate"), F.col("cd.DailyLimit"),
                F.lit("Has Card").alias("recommendation"),
            )
        )

        candidates_slim = cross_sell_candidates.select(
            "CustomerID",
            F.concat_ws(" ", "FirstName", "LastName").alias("customer_name"),
            "CustomerType",
            F.lit(None).cast("string").alias("CardID"),
            F.lit(None).cast("string").alias("CardType"),
            F.lit(None).cast("string").alias("card_status"),
            F.lit(None).cast("date").alias("ExpiryDate"),
            F.lit(None).cast("double").alias("DailyLimit"),
            "recommendation",
        )

        return adoption_summary.unionByName(candidates_slim).orderBy("CustomerID")

    # ------------------------------------------------------------------
    # Writer helper
    # ------------------------------------------------------------------

    def write_kpi(self, df: DataFrame, kpi_name: str, mode: str = "overwrite") -> None:
        table_id = f"{self.bq_dataset}.kpi_{kpi_name}"
        (
            df.write.format("bigquery")
            .option("table", table_id)
            .option("temporaryGcsBucket", self.gcs_temp_bucket)
            .mode(mode)
            .save()
        )
