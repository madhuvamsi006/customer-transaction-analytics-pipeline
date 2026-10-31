"""
Gold layer — fact table: fact_transactions.

Joins the Silver transactions DataFrame with Gold dimension surrogate keys
to produce a proper star-schema fact table.

Grain: one row per transaction (TransactionID).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.metadata.registry import MetadataRegistry


class FactBuilder:
    """
    Builds fact_transactions by joining Silver transactions to Gold dimensions.

    Parameters
    ----------
    spark          : active SparkSession
    registry       : MetadataRegistry
    bq_dataset     : BigQuery Gold dataset (e.g. "bank_gold")
    gcs_temp_bucket: GCS temp for Spark connector
    """

    def __init__(
        self,
        spark: SparkSession,
        registry: MetadataRegistry,
        bq_dataset: str,
        gcs_temp_bucket: str,
    ):
        self.spark = spark
        self.registry = registry
        self.bq_dataset = bq_dataset
        self.gcs_temp_bucket = gcs_temp_bucket

    def build_fact_transactions(
        self,
        transactions_df: DataFrame,
        dim_account_df: DataFrame,
        dim_customer_df: DataFrame,
        dim_branch_df: DataFrame,
        dim_product_df: DataFrame,
        dim_transaction_type_df: DataFrame,
    ) -> DataFrame:
        """
        Build fact_transactions.

        Grain: one row per TransactionID.
        Surrogate keys are resolved from current dimension records.
        """
        # Only current dimension records for key lookup
        cur_account = dim_account_df.filter(F.col("is_current") == True)
        cur_customer = dim_customer_df.filter(F.col("is_current") == True)

        fact = (
            transactions_df
            # Join account dimension → get account surrogate key + branch/product keys
            .join(
                cur_account.select(
                    F.col("AccountID"),
                    F.col("surrogate_key").alias("account_sk"),
                    F.col("CustomerID").alias("_acc_customer_id"),
                    F.col("BranchID").alias("_acc_branch_id"),
                    F.col("AccountTypeID").alias("_acc_type_id"),
                ),
                on="AccountID",
                how="left",
            )
            # Join customer dimension → customer surrogate key
            .join(
                cur_customer.select(
                    F.col("CustomerID"),
                    F.col("surrogate_key").alias("customer_sk"),
                ),
                cur_account.isNull()  # use _acc_customer_id when account found
                or transactions_df["AccountID"].isNull(),
                how="left",
            )
        )

        # Simpler sequential join pattern for clarity
        fact = transactions_df.alias("t")

        # Account dim
        fact = fact.join(
            cur_account.select("AccountID",
                               F.col("surrogate_key").alias("account_sk"),
                               "CustomerID", "BranchID", "AccountTypeID").alias("a"),
            F.col("t.AccountID") == F.col("a.AccountID"),
            "left",
        )

        # Customer dim (via account → CustomerID)
        fact = fact.join(
            cur_customer.select("CustomerID",
                                F.col("surrogate_key").alias("customer_sk")).alias("c"),
            F.col("a.CustomerID") == F.col("c.CustomerID"),
            "left",
        )

        # Branch dim
        fact = fact.join(
            dim_branch_df.select("BranchID",
                                 F.col("surrogate_key").alias("branch_sk")).alias("b"),
            F.col("a.BranchID") == F.col("b.BranchID"),
            "left",
        )

        # Product / AccountType dim
        fact = fact.join(
            dim_product_df.select("AccountTypeID",
                                  F.col("surrogate_key").alias("product_sk")).alias("p"),
            F.col("a.AccountTypeID") == F.col("p.AccountTypeID"),
            "left",
        )

        # TransactionType dim
        fact = fact.join(
            dim_transaction_type_df.select("TransactionTypeID",
                                           F.col("surrogate_key").alias("txn_type_sk")).alias("tt"),
            F.col("t.TransactionTypeID") == F.col("tt.TransactionTypeID"),
            "left",
        )

        fact_final = fact.select(
            F.col("t.TransactionID").alias("transaction_id"),
            F.col("account_sk"),
            F.col("customer_sk"),
            F.col("branch_sk"),
            F.col("product_sk"),
            F.col("txn_type_sk"),
            F.col("t.TransactionDate").alias("transaction_date"),
            F.col("t.Amount").alias("amount"),
            F.col("t.Currency").alias("currency"),
            F.col("t.Channel").alias("channel"),
            F.col("t.Description").alias("description"),
            F.col("t.Status").alias("status"),
            F.col("t.ReferenceNo").alias("reference_no"),
            F.current_timestamp().alias("_gold_load_timestamp"),
        )

        return fact_final

    def write_fact(self, fact_df: DataFrame, mode: str = "append") -> None:
        table_id = f"{self.bq_dataset}.fact_transactions"
        (
            fact_df.write.format("bigquery")
            .option("table", table_id)
            .option("temporaryGcsBucket", self.gcs_temp_bucket)
            .mode(mode)
            .save()
        )
