"""
Silver layer — type casting and cleansing.

Reads bronze DataFrames, applies type casts from the JSON schema,
trims whitespace, standardises date formats, and replaces empty strings
with NULL for nullable columns.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.metadata.registry import MetadataRegistry


# Maps JSON schema type strings → Spark types
_TYPE_MAP: Dict[str, T.DataType] = {
    "STRING": T.StringType(),
    "INTEGER": T.IntegerType(),
    "FLOAT": T.DoubleType(),
    "DATE": T.DateType(),
    "TIMESTAMP": T.TimestampType(),
    "BOOLEAN": T.BooleanType(),
}


class DataCleansing:
    """
    Applies schema-driven type casting and cleansing to a bronze DataFrame.

    Steps:
    1. Trim whitespace from all string columns.
    2. Cast columns to types defined in the entity schema.
    3. Replace empty strings with NULL for nullable fields.
    4. Add _silver_load_timestamp for lineage.
    """

    def __init__(self, registry: MetadataRegistry):
        self.registry = registry

    def cleanse(self, df: DataFrame, entity_name: str) -> DataFrame:
        """Return a cleansed Silver DataFrame for *entity_name*."""
        field_types = self.registry.get_field_types(entity_name)
        schema_fields = self._get_schema_fields(entity_name)
        nullable_fields = {
            f["name"] for f in schema_fields if f.get("nullable", True)
        }

        # Step 1: trim all string columns
        for col in df.columns:
            if isinstance(df.schema[col].dataType, T.StringType):
                df = df.withColumn(col, F.trim(F.col(col)))

        # Step 2: empty strings → NULL for nullable fields
        for field_name in nullable_fields:
            if field_name in df.columns:
                df = df.withColumn(
                    field_name,
                    F.when(F.col(field_name) == "", None).otherwise(F.col(field_name)),
                )

        # Step 3: type casting
        for field_name, type_str in field_types.items():
            if field_name not in df.columns:
                continue
            spark_type = _TYPE_MAP.get(type_str.upper())
            if spark_type is None:
                continue
            if isinstance(spark_type, T.DateType):
                # Parse ISO date strings
                df = df.withColumn(
                    field_name,
                    F.to_date(F.col(field_name), "yyyy-MM-dd"),
                )
            elif isinstance(spark_type, T.TimestampType):
                df = df.withColumn(
                    field_name,
                    F.to_timestamp(F.col(field_name)),
                )
            else:
                df = df.withColumn(field_name, F.col(field_name).cast(spark_type))

        # Step 4: add Silver timestamp
        df = df.withColumn(
            "_silver_load_timestamp", F.current_timestamp()
        )
        return df

    def _get_schema_fields(self, entity_name: str) -> List[Dict]:
        schema = self.registry.get_schema(entity_name)
        if schema is None:
            return []
        return schema.get("fields", [])
