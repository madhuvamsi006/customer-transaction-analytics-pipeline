"""
Gold layer — SCD Type 2 dimension processing.

For entities configured with scd_type: 2 (Customers, Accounts):
  - New natural keys → INSERT with is_current=True, eff_start_date=today, eff_end_date=NULL
  - Changed tracked columns → EXPIRE old record (eff_end_date=today, is_current=False),
    INSERT new version
  - Unchanged records → no-op

Surrogate key is a SHA-256 of (natural_key + eff_start_date).
All prior versions remain queryable via is_current=False.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType

from src.metadata.registry import MetadataRegistry


def _make_surrogate_key(natural_key: str, eff_start: str) -> str:
    return hashlib.sha256(f"{natural_key}|{eff_start}".encode()).hexdigest()[:16]


_SK_UDF = F.udf(
    lambda nk, es: _make_surrogate_key(str(nk), str(es)), StringType()
)


class DimensionBuilder:
    """
    Builds and maintains SCD Type 2 dimensions in BigQuery.

    Parameters
    ----------
    spark          : active SparkSession
    registry       : MetadataRegistry
    bq_dataset     : BigQuery dataset for gold tables (e.g. "bank_gold")
    gcs_temp_bucket: GCS temp bucket for BigQuery Spark connector
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

    def apply_scd2(
        self,
        incoming_df: DataFrame,
        entity_name: str,
        effective_date: Optional[str] = None,
    ) -> DataFrame:
        """
        Merge *incoming_df* into the Gold dimension table using SCD Type 2.

        Returns the updated full dimension DataFrame.
        """
        eff_date = effective_date or date.today().isoformat()
        pk = self.registry.get_primary_key(entity_name)
        track_cols = self.registry.get_scd_track_columns(entity_name)
        target_table = f"{self.bq_dataset}.dim_{entity_name.lower()}"

        # Prepare incoming with SCD columns
        incoming_clean = self._add_scd_columns(incoming_df, pk, eff_date, is_new=True)

        # Try to load existing dimension
        try:
            existing_df = (
                self.spark.read.format("bigquery")
                .option("table", target_table)
                .load()
            )
        except Exception:
            # First load — write all as current
            self._write_dimension(incoming_clean, target_table, mode="overwrite")
            return incoming_clean

        current_df = existing_df.filter(F.col("is_current") == True)
        historical_df = existing_df.filter(F.col("is_current") == False)

        # Detect changes
        changed_df, new_keys_df, unchanged_df = self._classify_incoming(
            incoming_clean, current_df, pk, track_cols
        )

        # Expire changed records in the existing current set
        expired_df = self._expire_records(current_df, changed_df, pk, eff_date)

        # New versions for changed records
        new_versions_df = changed_df  # already has is_current=True, new eff_start

        # Assemble final dimension
        full_dim = (
            historical_df
            .unionByName(expired_df, allowMissingColumns=True)
            .unionByName(unchanged_df, allowMissingColumns=True)
            .unionByName(new_versions_df, allowMissingColumns=True)
            .unionByName(new_keys_df, allowMissingColumns=True)
        )

        self._write_dimension(full_dim, target_table, mode="overwrite")
        return full_dim

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_scd_columns(
        self, df: DataFrame, pk: str, eff_date: str, is_new: bool
    ) -> DataFrame:
        df = df.withColumn("eff_start_date", F.lit(eff_date).cast("date"))
        df = df.withColumn("eff_end_date", F.lit(None).cast("date"))
        df = df.withColumn("is_current", F.lit(True).cast(BooleanType()))
        df = df.withColumn(
            "surrogate_key",
            _SK_UDF(F.col(pk).cast("string"), F.lit(eff_date)),
        )
        return df

    def _classify_incoming(
        self,
        incoming_df: DataFrame,
        current_df: DataFrame,
        pk: str,
        track_cols: List[str],
    ):
        """
        Split incoming rows into:
        - changed_df  : rows where tracked columns differ from current
        - new_keys_df : rows with a primary key not in the current dimension
        - unchanged_df: rows from current that have not changed (kept as-is)
        """
        inc_cols = [pk] + [c for c in track_cols if c in incoming_df.columns]
        cur_cols = [pk] + [c for c in track_cols if c in current_df.columns]

        inc_slim = incoming_df.select(inc_cols).alias("inc")
        cur_slim = current_df.select(cur_cols + ["surrogate_key", "eff_start_date",
                                                  "eff_end_date", "is_current"]).alias("cur")

        joined = inc_slim.join(cur_slim, on=pk, how="left")

        # Build change condition: any tracked column differs
        change_cond = F.lit(False)
        for col in track_cols:
            if col in inc_slim.columns and col in cur_slim.columns:
                change_cond = change_cond | (
                    F.col(f"inc.{col}") != F.col(f"cur.{col}")
                )

        changed_keys = (
            joined.filter(
                F.col("cur." + pk).isNotNull() & change_cond
            )
            .select(F.col(f"inc.{pk}").alias(pk))
        )
        new_keys = (
            joined.filter(F.col("cur." + pk).isNull())
            .select(F.col(f"inc.{pk}").alias(pk))
        )

        changed_df = incoming_df.join(changed_keys, on=pk, how="inner")
        new_keys_df = incoming_df.join(new_keys, on=pk, how="inner")
        unchanged_df = current_df.join(
            changed_keys.union(new_keys), on=pk, how="left_anti"
        )
        return changed_df, new_keys_df, unchanged_df

    def _expire_records(
        self,
        current_df: DataFrame,
        changed_df: DataFrame,
        pk: str,
        eff_date: str,
    ) -> DataFrame:
        """Set eff_end_date and is_current=False on records being superseded."""
        changed_keys = changed_df.select(pk).distinct()
        to_expire = current_df.join(changed_keys, on=pk, how="inner")
        still_current = current_df.join(changed_keys, on=pk, how="left_anti")

        expired = (
            to_expire
            .withColumn("eff_end_date", F.lit(eff_date).cast("date"))
            .withColumn("is_current", F.lit(False).cast(BooleanType()))
        )
        return expired

    def _write_dimension(
        self, df: DataFrame, table_id: str, mode: str = "append"
    ) -> None:
        (
            df.write.format("bigquery")
            .option("table", table_id)
            .option("temporaryGcsBucket", self.gcs_temp_bucket)
            .mode(mode)
            .save()
        )
