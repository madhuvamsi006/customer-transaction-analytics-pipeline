"""Unit tests for Silver layer: DQ checks and deduplication."""

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    from src.metadata.registry import MetadataRegistry
    from src.silver.dq_checks import DQChecker
    from src.silver.deduplication import Deduplicator

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@pytest.fixture(scope="session")
def spark():
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not installed")
    return (
        SparkSession.builder.master("local[1]")
        .appName("test-silver")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def registry(tmp_path_factory):
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not installed")
    import shutil
    src = __import__("pathlib").Path(__file__).parent.parent / "config"
    dst = tmp_path_factory.mktemp("config")
    shutil.copytree(str(src), str(dst / "config"))
    return MetadataRegistry(config_dir=str(dst / "config"))


class TestDQCheckerNotNull:
    def test_missing_email_flagged(self, spark, registry):
        schema = StructType([
            StructField("CustomerID", IntegerType()),
            StructField("Email", StringType()),
            StructField("Phone", StringType()),
            StructField("_load_timestamp", StringType()),
        ])
        data = [(1, None, "+1-555-0101", "2024-01-01"),
                (2, "bob@test.com", "+1-555-0102", "2024-01-01")]
        df = spark.createDataFrame(data, schema)

        checker = DQChecker(registry)
        clean, quarantine = checker.run(df, "Customers")
        assert quarantine.count() >= 1
        rule_ids = {r["_dq_rule_id"] for r in quarantine.collect()}
        assert "VR002" in rule_ids


class TestDeduplicator:
    def test_duplicate_removed(self, spark, registry):
        schema = StructType([
            StructField("CustomerID", IntegerType()),
            StructField("Name", StringType()),
            StructField("_load_timestamp", StringType()),
        ])
        data = [(1, "Alice", "2024-01-01"), (1, "Alice", "2024-01-02")]
        df = spark.createDataFrame(data, schema)

        dedup = Deduplicator(registry)
        result = dedup.deduplicate(df, "Customers")
        assert result.count() == 1

    def test_latest_wins(self, spark, registry):
        schema = StructType([
            StructField("CustomerID", IntegerType()),
            StructField("Name", StringType()),
            StructField("_load_timestamp", StringType()),
        ])
        # Two rows for same key; latest should win
        data = [(1, "Alice-old", "2024-01-01"), (1, "Alice-new", "2024-01-02")]
        df = spark.createDataFrame(data, schema)

        dedup = Deduplicator(registry)
        result = dedup.deduplicate(df, "Customers")
        assert result.collect()[0]["Name"] == "Alice-new"


class TestDQHighValueThreshold:
    def test_high_value_flagged(self, spark, registry):
        schema = StructType([
            StructField("TransactionID", StringType()),
            StructField("AccountID", IntegerType()),
            StructField("Amount", DoubleType()),
            StructField("Currency", StringType()),
            StructField("_load_timestamp", StringType()),
        ])
        data = [("T001", 101, 150000.0, "INR", "2024-01-01"),
                ("T002", 102, 5000.0, "INR", "2024-01-01")]
        df = spark.createDataFrame(data, schema)

        checker = DQChecker(registry)
        clean, quarantine = checker.run(df, "Transactions")
        q_ids = {r["TransactionID"] for r in quarantine.collect()}
        assert "T001" in q_ids
        assert "T002" not in q_ids
