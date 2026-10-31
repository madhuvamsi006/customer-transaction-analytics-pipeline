"""
Cloud Dataproc / PySpark entry-point pipeline.

Orchestrates Bronze → Silver → Gold processing for one load day.

Usage (local Spark):
    spark-submit pipeline/dataproc_pipeline.py \
        --input_path data/day0.txt \
        --batch_id BATCH_20240101_D0 \
        --load_day 0 \
        --bq_dataset bank_analytics \
        --gcs_bucket my-temp-bucket \
        --project my-gcp-project

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark pipeline/dataproc_pipeline.py \
        --cluster=<cluster> --region=<region> \
        -- --input_path gs://bucket/day0.txt ...
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession

from src.bronze.ingestion import BronzeIngestion
from src.gold.dimensions import DimensionBuilder
from src.gold.facts import FactBuilder
from src.gold.kpis import KPIBuilder
from src.metadata.registry import MetadataRegistry
from src.silver.cleansing import DataCleansing
from src.silver.deduplication import Deduplicator
from src.silver.dq_checks import DQChecker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Customer Transaction Analytics Pipeline")
    parser.add_argument("--input_path", required=True, help="Path to source file (local or gs://)")
    parser.add_argument("--batch_id", required=True, help="Unique batch identifier")
    parser.add_argument("--load_day", type=int, choices=[0, 1, 2], default=0,
                        help="Load day: 0=full baseline, 1/2=incremental")
    parser.add_argument("--bq_dataset", default="bank_analytics", help="BigQuery dataset")
    parser.add_argument("--gcs_bucket", required=True, help="GCS temp bucket for BQ connector")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--effective_date", default=None,
                        help="Effective date for SCD-2 (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--config_dir", default=None, help="Path to config directory")
    return parser.parse_args()


def build_spark_session(project: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("CustomerTransactionAnalyticsPipeline")
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
        .config("parentProject", project)
        .getOrCreate()
    )


def run(args: argparse.Namespace) -> None:
    spark = build_spark_session(args.project)
    registry = MetadataRegistry(config_dir=args.config_dir)
    effective_date = args.effective_date or date.today().isoformat()

    logger.info("=== BRONZE LAYER ===")
    bronze = BronzeIngestion(spark, registry, args.bq_dataset, args.gcs_bucket)
    bronze_stats = bronze.ingest_file(args.input_path, args.batch_id, effective_date)
    logger.info("Bronze ingestion stats: %s", bronze_stats)

    logger.info("=== SILVER LAYER ===")
    cleansing = DataCleansing(registry)
    deduplicator = Deduplicator(registry)

    silver_frames = {}
    for entity_name in registry.list_entities():
        bronze_table = f"{args.bq_dataset}.bronze_{entity_name.lower()}"
        try:
            raw_df = (
                spark.read.format("bigquery")
                .option("table", bronze_table)
                .load()
            )
        except Exception:
            logger.warning("Bronze table %s not found, skipping.", bronze_table)
            continue

        clean_df = cleansing.cleanse(raw_df, entity_name)
        deduped_df = deduplicator.deduplicate(clean_df, entity_name)
        silver_frames[entity_name] = deduped_df
        logger.info("Silver: %s → %d rows after dedup", entity_name, deduped_df.count())

    logger.info("=== SILVER DQ CHECKS ===")
    dq_checker = DQChecker(registry, reference_frames=silver_frames)
    clean_silver = {}
    for entity_name, df in silver_frames.items():
        clean_df, quarantine_df = dq_checker.run(df, entity_name)
        clean_silver[entity_name] = clean_df
        qcount = quarantine_df.count()
        if qcount > 0:
            logger.warning("DQ: %s has %d quarantined rows", entity_name, qcount)
            quarantine_df.write.format("bigquery") \
                .option("table", f"{args.bq_dataset}.quarantine_{entity_name.lower()}") \
                .option("temporaryGcsBucket", args.gcs_bucket) \
                .mode("append").save()

    logger.info("=== GOLD LAYER — DIMENSIONS ===")
    dim_builder = DimensionBuilder(spark, registry, args.bq_dataset, args.gcs_bucket)

    for entity_name in ["Customers", "Accounts"]:
        if entity_name in clean_silver:
            dim_builder.apply_scd2(clean_silver[entity_name], entity_name, effective_date)
            logger.info("SCD-2 applied for %s", entity_name)

    # SCD-1 dimensions (Branches, AccountTypes, TransactionTypes)
    for entity_name in ["Branches", "AccountTypes", "TransactionTypes"]:
        if entity_name not in clean_silver:
            continue
        table_name = f"{args.bq_dataset}.dim_{entity_name.lower()}"
        df = clean_silver[entity_name]
        from src.gold.dimensions import _SK_UDF
        import pyspark.sql.functions as F
        pk = registry.get_primary_key(entity_name)
        df = df.withColumn("surrogate_key", _SK_UDF(F.col(pk).cast("string"), F.lit(effective_date)))
        df.write.format("bigquery") \
            .option("table", table_name) \
            .option("temporaryGcsBucket", args.gcs_bucket) \
            .mode("overwrite").save()

    logger.info("=== GOLD LAYER — FACTS ===")
    if "Transactions" in clean_silver:
        fact_builder = FactBuilder(spark, registry, args.bq_dataset, args.gcs_bucket)

        def load_dim(name: str):
            try:
                return spark.read.format("bigquery").option(
                    "table", f"{args.bq_dataset}.dim_{name.lower()}"
                ).load()
            except Exception:
                return spark.createDataFrame([], schema=None)

        fact_df = fact_builder.build_fact_transactions(
            transactions_df=clean_silver["Transactions"],
            dim_account_df=load_dim("accounts"),
            dim_customer_df=load_dim("customers"),
            dim_branch_df=load_dim("branches"),
            dim_product_df=load_dim("accounttypes"),
            dim_transaction_type_df=load_dim("transactiontypes"),
        )
        fact_builder.write_fact(fact_df, mode="append" if args.load_day > 0 else "overwrite")

    logger.info("=== GOLD LAYER — KPIs ===")
    try:
        fact_df = spark.read.format("bigquery").option(
            "table", f"{args.bq_dataset}.fact_transactions"
        ).load()
        dim_customer = spark.read.format("bigquery").option(
            "table", f"{args.bq_dataset}.dim_customers"
        ).load()
        dim_account = spark.read.format("bigquery").option(
            "table", f"{args.bq_dataset}.dim_accounts"
        ).load()

        kpi = KPIBuilder(spark, args.bq_dataset, args.gcs_bucket)

        kpi.write_kpi(kpi.top_5_customers_by_volume(fact_df, dim_customer), "top5_customers")
        kpi.write_kpi(
            kpi.product_revenue_contribution(
                fact_df,
                spark.read.format("bigquery").option(
                    "table", f"{args.bq_dataset}.dim_accounttypes"
                ).load(),
            ),
            "product_revenue",
        )
        kpi.write_kpi(
            kpi.inactive_accounts_90days(fact_df, dim_account, effective_date),
            "inactive_accounts",
        )
        kpi.write_kpi(
            kpi.suspicious_transactions(fact_df, dim_customer, dim_account),
            "suspicious_transactions",
        )
        kpi.write_kpi(kpi.customer_scd2_history(dim_customer), "customer_scd2_history")
        if "Loans" in clean_silver and "LoanPayments" in clean_silver:
            kpi.write_kpi(
                kpi.loan_portfolio_health(
                    clean_silver["Loans"], clean_silver["LoanPayments"],
                    dim_customer, None, dim_account,
                ),
                "loan_portfolio_health",
            )
        if "Cards" in clean_silver:
            kpi.write_kpi(
                kpi.card_adoption(clean_silver["Cards"], dim_customer),
                "card_adoption",
            )
    except Exception as e:
        logger.error("KPI computation failed: %s", e)

    logger.info("Pipeline completed for load_day=%d, batch_id=%s", args.load_day, args.batch_id)
    spark.stop()


if __name__ == "__main__":
    run(parse_args())
