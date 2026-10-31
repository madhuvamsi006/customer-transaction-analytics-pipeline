"""
Silver layer — data quality checks.

Applies all validation rules from validation_rules.yaml to a Silver DataFrame.
Returns (clean_df, quarantine_df).  The quarantine DataFrame carries three
extra columns: _dq_rule_id, _dq_rule_name, _dq_failure_reason.

Check types implemented
-----------------------
not_null        : one or more columns must not be NULL
deduplication   : flag/suppress exact duplicate rows by key columns
threshold       : numeric column must not exceed a static threshold value
referential     : column value must match a reference DataFrame column
date_range      : date column must fall within ref open/close dates
prefix_match    : column value must equal the ref entity's column value
structuring     : near-threshold deposits via Branch/ATM within a rolling window
dormant_burst   : long inactivity followed by clustered activity
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType

from src.metadata.registry import MetadataRegistry


class DQChecker:
    """
    Applies entity-level data-quality rules and splits a DataFrame into
    clean and quarantine sets.

    Parameters
    ----------
    registry         : MetadataRegistry
    reference_frames : dict of entity_name → Silver DataFrame, used for
                       referential / date-range / prefix-match checks.
    """

    def __init__(
        self,
        registry: MetadataRegistry,
        reference_frames: Optional[Dict[str, DataFrame]] = None,
    ):
        self.registry = registry
        self.ref_frames: Dict[str, DataFrame] = reference_frames or {}

    def run(
        self, df: DataFrame, entity_name: str
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Apply all configured rules for *entity_name*.

        Returns
        -------
        (clean_df, quarantine_df)
        clean_df      : rows that passed all rules
        quarantine_df : rows that failed at least one rule, annotated with
                        _dq_rule_id, _dq_rule_name, _dq_failure_reason
        """
        rules = self.registry.get_rules_for_entity(entity_name)

        # Accumulate failure flag columns (one per rule)
        working_df = df
        quarantine_parts: List[DataFrame] = []

        for rule in rules:
            failed_df = self._apply_rule(working_df, rule, entity_name)
            if failed_df is None:
                continue

            annotated = (
                failed_df
                .withColumn("_dq_rule_id", F.lit(rule["rule_id"]))
                .withColumn("_dq_rule_name", F.lit(rule["rule_name"]))
                .withColumn("_dq_failure_reason", F.lit(rule.get("description", "")))
            )
            quarantine_parts.append(annotated)

            # Remove quarantined rows from the working dataset
            pk = self.registry.get_primary_key(entity_name)
            if pk in working_df.columns:
                bad_keys = failed_df.select(pk).distinct()
                working_df = working_df.join(bad_keys, on=pk, how="left_anti")

        if quarantine_parts:
            # Union all quarantine frames; add missing DQ cols to base df
            first = quarantine_parts[0]
            rest = quarantine_parts[1:]
            quarantine_df = first
            for part in rest:
                quarantine_df = quarantine_df.unionByName(part)
        else:
            # Empty quarantine DataFrame with the same schema + DQ cols
            quarantine_df = df.limit(0).withColumn(
                "_dq_rule_id", F.lit("").cast(StringType())
            ).withColumn(
                "_dq_rule_name", F.lit("").cast(StringType())
            ).withColumn(
                "_dq_failure_reason", F.lit("").cast(StringType())
            )

        return working_df, quarantine_df

    # ------------------------------------------------------------------
    # Rule dispatchers
    # ------------------------------------------------------------------

    def _apply_rule(
        self, df: DataFrame, rule: Dict, entity_name: str
    ) -> Optional[DataFrame]:
        """Return rows that FAIL *rule*, or None if rule type is unrecognised."""
        check_type = rule.get("check_type")

        if check_type == "not_null":
            return self._check_not_null(df, rule)

        if check_type == "deduplication":
            return self._check_deduplication(df, rule)

        if check_type == "threshold":
            return self._check_threshold(df, rule)

        if check_type == "referential":
            return self._check_referential(df, rule)

        if check_type == "date_range":
            return self._check_date_range(df, rule)

        if check_type == "prefix_match":
            return self._check_prefix_match(df, rule)

        if check_type == "structuring":
            return self._check_structuring(df, rule)

        if check_type == "dormant_burst":
            return self._check_dormant_burst(df, rule)

        return None

    # ------------------------------------------------------------------
    # Individual check implementations
    # ------------------------------------------------------------------

    def _check_not_null(self, df: DataFrame, rule: Dict) -> DataFrame:
        columns = rule.get("columns", [])
        condition = F.lit(False)
        for col in columns:
            if col in df.columns:
                condition = condition | F.col(col).isNull()
        return df.filter(condition)

    def _check_deduplication(self, df: DataFrame, rule: Dict) -> DataFrame:
        columns = rule.get("columns", [])
        existing_cols = [c for c in columns if c in df.columns]
        if not existing_cols:
            return df.limit(0)
        window = Window.partitionBy(*existing_cols).orderBy(F.lit(1))
        return (
            df.withColumn("_dup_rank", F.row_number().over(window))
            .filter(F.col("_dup_rank") > 1)
            .drop("_dup_rank")
        )

    def _check_threshold(self, df: DataFrame, rule: Dict) -> DataFrame:
        col = rule.get("column")
        threshold = rule.get("threshold_value")
        if col not in df.columns or threshold is None:
            return df.limit(0)
        return df.filter(F.col(col).cast("double") > float(threshold))

    def _check_referential(self, df: DataFrame, rule: Dict) -> DataFrame:
        ref_entity = rule.get("ref_entity")
        ref_col = rule.get("ref_column")
        join_col = rule.get("join_column")
        compare_col = rule.get("compare_column")

        ref_df = self.ref_frames.get(ref_entity)
        if ref_df is None or join_col not in df.columns:
            return df.limit(0)

        ref_slim = ref_df.select(
            F.col(join_col).alias("_ref_join"),
            F.col(ref_col).alias("_ref_val"),
        )
        joined = df.join(ref_slim, df[join_col] == F.col("_ref_join"), "left")
        return (
            joined.filter(
                F.col("_ref_val").isNotNull()
                & (F.col(compare_col) != F.col("_ref_val"))
            )
            .drop("_ref_join", "_ref_val")
        )

    def _check_date_range(self, df: DataFrame, rule: Dict) -> DataFrame:
        date_col = rule.get("date_column")
        ref_entity = rule.get("ref_entity")
        ref_open = rule.get("ref_open_col")
        ref_close = rule.get("ref_close_col")
        join_col = "AccountID"

        ref_df = self.ref_frames.get(ref_entity)
        if ref_df is None or date_col not in df.columns:
            return df.limit(0)

        ref_slim = ref_df.select(
            F.col(join_col).alias("_ref_join"),
            F.col(ref_open).alias("_open"),
            F.col(ref_close).alias("_close"),
        )
        joined = df.join(ref_slim, df[join_col] == F.col("_ref_join"), "left")
        failed = joined.filter(
            (F.col(date_col) < F.col("_open"))
            | (F.col("_close").isNotNull() & (F.col(date_col) > F.col("_close")))
        ).drop("_ref_join", "_open", "_close")
        return failed

    def _check_prefix_match(self, df: DataFrame, rule: Dict) -> DataFrame:
        col = rule.get("column")
        ref_entity = rule.get("ref_entity")
        ref_col = rule.get("ref_column")
        join_col = rule.get("join_column")

        ref_df = self.ref_frames.get(ref_entity)
        if ref_df is None or col not in df.columns:
            return df.limit(0)

        ref_slim = ref_df.select(
            F.col(join_col).alias("_ref_join"),
            F.col(ref_col).alias("_ref_sc"),
        )
        joined = df.join(ref_slim, df[join_col] == F.col("_ref_join"), "left")
        return (
            joined.filter(
                F.col("_ref_sc").isNotNull()
                & (F.col(col) != F.col("_ref_sc"))
            )
            .drop("_ref_join", "_ref_sc")
        )

    def _check_structuring(self, df: DataFrame, rule: Dict) -> DataFrame:
        """
        Flag accounts with >= min_count near-threshold Deposit transactions
        via Branch or ATM within a rolling window_days window.

        'Near-threshold' is defined as amount >= near_threshold * (1 - tolerance).
        Cash deposit proxy assumption: Channel IN ('Branch', 'ATM').
        """
        amount_col = rule.get("column", "Amount")
        near_threshold = float(rule.get("near_threshold", 100000))
        tolerance = float(rule.get("tolerance", 0.05))
        window_days = int(rule.get("window_days", 3))
        min_count = int(rule.get("min_count", 2))
        channels = rule.get("channel_filter", ["Branch", "ATM"])

        lower_bound = near_threshold * (1 - tolerance)
        upper_bound = near_threshold  # strictly below threshold

        if amount_col not in df.columns:
            return df.limit(0)

        candidate = df.filter(
            F.col(amount_col).cast("double").between(lower_bound, upper_bound)
            & F.col("Channel").isin(channels)
        )
        if candidate.count() == 0:
            return df.limit(0)

        # Count near-threshold deposits per account within rolling window
        w = (
            Window.partitionBy("AccountID")
            .orderBy(F.col("TransactionDate").cast("timestamp").cast("long"))
            .rangeBetween(0, window_days * 86400)
        )
        flagged_accounts = (
            candidate.withColumn("_near_cnt", F.count("*").over(w))
            .filter(F.col("_near_cnt") >= min_count)
            .select("AccountID")
            .distinct()
        )
        return df.join(flagged_accounts, on="AccountID", how="inner")

    def _check_dormant_burst(self, df: DataFrame, rule: Dict) -> DataFrame:
        """
        Flag accounts that had >= inactivity_days gap then >= burst_min_transactions
        within burst_window_days.
        """
        inactivity_days = int(rule.get("inactivity_days", 90))
        burst_window = int(rule.get("burst_window_days", 7))
        burst_min = int(rule.get("burst_min_transactions", 3))

        if "TransactionDate" not in df.columns or "AccountID" not in df.columns:
            return df.limit(0)

        w = Window.partitionBy("AccountID").orderBy("TransactionDate")
        df_gaps = df.withColumn(
            "_prev_date",
            F.lag("TransactionDate").over(w),
        ).withColumn(
            "_gap_days",
            F.datediff(F.col("TransactionDate"), F.col("_prev_date")),
        )

        # Accounts that had a long gap
        dormant_accounts = (
            df_gaps.filter(F.col("_gap_days") >= inactivity_days)
            .select("AccountID", "TransactionDate")
            .withColumnRenamed("TransactionDate", "_reactivation_date")
        )
        if dormant_accounts.count() == 0:
            return df.limit(0)

        # Check burst after reactivation
        joined = df.join(dormant_accounts, on="AccountID", how="inner")
        burst_w = (
            Window.partitionBy("AccountID")
            .orderBy(F.col("TransactionDate").cast("timestamp").cast("long"))
            .rangeBetween(0, burst_window * 86400)
        )
        burst_flagged = (
            joined.filter(F.col("TransactionDate") >= F.col("_reactivation_date"))
            .withColumn("_burst_cnt", F.count("*").over(burst_w))
            .filter(F.col("_burst_cnt") >= burst_min)
            .select("AccountID")
            .distinct()
        )
        return df.join(burst_flagged, on="AccountID", how="inner")
