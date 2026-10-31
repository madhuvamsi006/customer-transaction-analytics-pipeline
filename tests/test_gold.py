"""Unit tests for Gold layer: SCD Type 2 dimension builder."""

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, DateType, IntegerType, StringType, StructField, StructType

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@pytest.fixture(scope="session")
def spark():
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not installed")
    return (
        SparkSession.builder.master("local[1]")
        .appName("test-gold")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def customer_schema():
    return StructType([
        StructField("CustomerID", IntegerType()),
        StructField("FirstName", StringType()),
        StructField("LastName", StringType()),
        StructField("CustomerType", StringType()),
        StructField("Status", StringType()),
        StructField("Email", StringType()),
    ])


@pytest.fixture
def scd2_schema():
    return StructType([
        StructField("CustomerID", IntegerType()),
        StructField("FirstName", StringType()),
        StructField("LastName", StringType()),
        StructField("CustomerType", StringType()),
        StructField("Status", StringType()),
        StructField("Email", StringType()),
        StructField("surrogate_key", StringType()),
        StructField("eff_start_date", DateType()),
        StructField("eff_end_date", DateType()),
        StructField("is_current", BooleanType()),
    ])


class TestSCD2Logic:
    """
    Tests for the SCD Type 2 classification logic without requiring BigQuery.
    We test the helper functions in DimensionBuilder directly.
    """

    def test_new_key_detected_as_new(self, spark, customer_schema, scd2_schema):
        """A CustomerID not in the existing dimension is classified as 'new'."""
        from src.gold.dimensions import DimensionBuilder
        from unittest.mock import MagicMock

        registry = MagicMock()
        registry.get_primary_key.return_value = "CustomerID"
        registry.get_scd_track_columns.return_value = ["CustomerType", "Status"]

        builder = DimensionBuilder(spark, registry, "bank_gold", "bucket")

        incoming_data = [(369, "New", "Cust", "Retail", "Active", "new@test.com")]
        incoming_df = spark.createDataFrame(incoming_data, customer_schema)

        # Build a minimal existing dim (current record for a different customer)
        existing_data = [(1, "Alice", "Johnson", "Retail", "Active", "alice@test.com",
                          "surr_001", __import__("datetime").date(2024, 1, 1), None, True)]
        existing_df = spark.createDataFrame(existing_data, scd2_schema)

        incoming_with_scd = builder._add_scd_columns(incoming_df, "CustomerID", "2024-01-15", is_new=True)
        changed, new_keys, unchanged = builder._classify_incoming(
            incoming_with_scd, existing_df, "CustomerID", ["CustomerType", "Status"]
        )

        assert new_keys.count() == 1
        assert changed.count() == 0

    def test_changed_record_detected(self, spark, customer_schema, scd2_schema):
        """A CustomerID present but with different tracked column is 'changed'."""
        from src.gold.dimensions import DimensionBuilder
        from unittest.mock import MagicMock

        registry = MagicMock()
        registry.get_primary_key.return_value = "CustomerID"
        registry.get_scd_track_columns.return_value = ["CustomerType", "Status"]

        builder = DimensionBuilder(spark, registry, "bank_gold", "bucket")

        # Incoming: CustomerID=1 with CustomerType changed to 'Premium'
        incoming_data = [(1, "Alice", "Johnson-Smith", "Premium", "Active", "alice@test.com")]
        incoming_df = spark.createDataFrame(incoming_data, customer_schema)

        existing_data = [(1, "Alice", "Johnson", "Retail", "Active", "alice@test.com",
                          "surr_001", __import__("datetime").date(2024, 1, 1), None, True)]
        existing_df = spark.createDataFrame(existing_data, scd2_schema)

        incoming_with_scd = builder._add_scd_columns(incoming_df, "CustomerID", "2024-01-15", is_new=True)
        changed, new_keys, unchanged = builder._classify_incoming(
            incoming_with_scd, existing_df, "CustomerID", ["CustomerType", "Status"]
        )

        assert changed.count() == 1
        assert new_keys.count() == 0

    def test_expire_sets_is_current_false(self, spark, scd2_schema):
        """Expiring a record sets is_current=False and eff_end_date."""
        from src.gold.dimensions import DimensionBuilder
        from unittest.mock import MagicMock

        registry = MagicMock()
        builder = DimensionBuilder(spark, registry, "bank_gold", "bucket")

        current_data = [(1, "Alice", "Johnson", "Retail", "Active", "alice@test.com",
                         "surr_001", __import__("datetime").date(2024, 1, 1), None, True)]
        current_df = spark.createDataFrame(current_data, scd2_schema)

        changed_schema = StructType([StructField("CustomerID", IntegerType())])
        changed_df = spark.createDataFrame([(1,)], changed_schema)

        expired = builder._expire_records(current_df, changed_df, "CustomerID", "2024-01-15")
        row = expired.collect()[0]
        assert row["is_current"] == False
        assert str(row["eff_end_date"]) == "2024-01-15"
