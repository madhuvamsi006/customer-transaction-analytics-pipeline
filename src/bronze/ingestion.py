"""
Bronze layer — replay-safe raw ingestion using PySpark.

Reads an interleaved source file, splits it into per-entity DataFrames,
adds pipeline metadata columns, and writes to BigQuery bronze tables.

Replay safety: each row is hashed; rows whose hash already exists in the
target table are skipped so re-processing the same file is idempotent.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.metadata.registry import MetadataRegistry
from src.parser.interleaved_parser import InterleavedFileParser


# Bronze metadata columns added to every raw row
BRONZE_META_COLS = [
    "_source_file",
    "_batch_id",
    "_load_timestamp",
    "_entity_name",
    "_row_hash",
]


def _compute_row_hash(row_values: List[str]) -> str:
    """SHA-256 of pipe-joined cell values for dedup / replay detection."""
    content = "|".join(str(v) for v in row_values)
    return hashlib.sha256(content.encode()).hexdigest()


def _build_schema(columns: List[str]) -> StructType:
    """All bronze columns are STRING; casting happens in Silver."""
    return StructType([StructField(c, StringType(), True) for c in columns])


class BronzeIngestion:
    """
    Ingests one source file (any load day) into BigQuery bronze tables.

    Parameters
    ----------
    spark        : active SparkSession
    registry     : MetadataRegistry instance
    bq_dataset   : BigQuery dataset for bronze tables  (e.g. "bank_bronze")
    gcs_temp_bucket : GCS bucket used by Spark BigQuery connector as temp storage
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
        self.parser = InterleavedFileParser()

    def ingest_file(
        self,
        file_path: str,
        batch_id: str,
        load_date: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Parse *file_path*, write each entity block to its bronze table.

        Returns dict of entity_name → rows written (after dedup / replay filter).
        """
        load_ts = load_date or datetime.utcnow().isoformat()
        entities = self.parser.parse(file_path)
        stats: Dict[str, int] = {}

        for entity_name, parsed in entities.items():
            if not parsed.rows:
                stats[entity_name] = 0
                continue

            df = self._build_bronze_df(
                entity_name, parsed.columns, parsed.rows,
                file_path, batch_id, load_ts,
            )
            written = self._write_with_replay_safety(entity_name, df)
            stats[entity_name] = written

        return stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_bronze_df(
        self,
        entity_name: str,
        columns: List[str],
        rows: List[Dict],
        source_file: str,
        batch_id: str,
        load_ts: str,
    ) -> DataFrame:
        """Build a Spark DataFrame for *entity_name* with bronze metadata columns."""
        schema = _build_schema(columns)
        data = [[row.get(c, "") for c in columns] for row in rows]

        # Compute row hash on the raw data values only (no meta columns)
        hashes = [_compute_row_hash(record) for record in data]

        rdd = self.spark.sparkContext.parallelize(
            [record + [source_file, batch_id, load_ts, entity_name, h]
             for record, h in zip(data, hashes)]
        )

        all_cols = columns + BRONZE_META_COLS
        full_schema = StructType(
            list(schema.fields)
            + [StructField(c, StringType(), True) for c in BRONZE_META_COLS]
        )
        return self.spark.createDataFrame(rdd, full_schema)

    def _write_with_replay_safety(self, entity_name: str, df: DataFrame) -> int:
        """
        Write *df* to the bronze BigQuery table for *entity_name*, skipping
        rows whose _row_hash is already present (replay-safety).

        Returns the number of rows actually written.
        """
        table_id = f"{self.bq_dataset}.bronze_{entity_name.lower()}"

        # Attempt to load existing hashes for dedup
        existing_hashes: set = set()
        try:
            existing_df = (
                self.spark.read.format("bigquery")
                .option("table", table_id)
                .load()
                .select("_row_hash")
            )
            existing_hashes = {r["_row_hash"] for r in existing_df.collect()}
        except Exception:
            # Table may not exist yet on first run — that's fine
            pass

        if existing_hashes:
            broadcast_hashes = self.spark.sparkContext.broadcast(existing_hashes)
            new_df = df.filter(
                ~F.col("_row_hash").isin(list(broadcast_hashes.value))
            )
        else:
            new_df = df

        count = new_df.count()
        if count > 0:
            (
                new_df.write.format("bigquery")
                .option("table", table_id)
                .option("temporaryGcsBucket", self.gcs_temp_bucket)
                .mode("append")
                .save()
            )
        return count
