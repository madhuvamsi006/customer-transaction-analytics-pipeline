"""
Silver layer — deduplication.

Keeps the latest record per natural primary key, determined by
_load_timestamp (bronze ingestion time).  For SCD-2 entities this is
a within-batch dedup only; cross-batch history is managed in Gold.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.metadata.registry import MetadataRegistry


class Deduplicator:
    """
    Deduplicates a Silver DataFrame using the entity's primary key.

    Uses a window function (row_number ordered by _load_timestamp DESC)
    so the most-recently-ingested record wins when the same key appears
    multiple times in one batch.
    """

    def __init__(self, registry: MetadataRegistry):
        self.registry = registry

    def deduplicate(self, df: DataFrame, entity_name: str) -> DataFrame:
        """Return a deduplicated DataFrame for *entity_name*."""
        pk = self.registry.get_primary_key(entity_name)

        if "_load_timestamp" not in df.columns:
            # Bronze metadata column may be named differently; fall back to row_number only
            return df.dropDuplicates([pk])

        window = Window.partitionBy(pk).orderBy(F.col("_load_timestamp").desc())
        deduped = (
            df.withColumn("_dedup_rank", F.row_number().over(window))
            .filter(F.col("_dedup_rank") == 1)
            .drop("_dedup_rank")
        )
        return deduped

    def flag_duplicates(self, df: DataFrame, entity_name: str) -> DataFrame:
        """
        Add a boolean column _is_duplicate that marks all but the winning
        record for each primary-key group. Useful for audit output.
        """
        pk = self.registry.get_primary_key(entity_name)
        window = Window.partitionBy(pk).orderBy(F.col("_load_timestamp").desc())
        return df.withColumn(
            "_is_duplicate",
            F.when(F.row_number().over(window) > 1, True).otherwise(False),
        )
