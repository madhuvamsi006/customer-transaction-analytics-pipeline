"""Unit tests for Bronze ingestion layer."""

import hashlib
from unittest.mock import MagicMock, patch

import pytest

from src.bronze.ingestion import BronzeIngestion, _compute_row_hash


class TestRowHash:
    def test_deterministic(self):
        values = ["1", "Alice", "Johnson", "alice@test.com"]
        assert _compute_row_hash(values) == _compute_row_hash(values)

    def test_different_values_differ(self):
        h1 = _compute_row_hash(["1", "Alice"])
        h2 = _compute_row_hash(["1", "Bob"])
        assert h1 != h2

    def test_is_sha256_hex(self):
        h = _compute_row_hash(["x"])
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_empty_values(self):
        h = _compute_row_hash([])
        assert isinstance(h, str) and len(h) == 64


class TestBronzeIngestion:
    @pytest.fixture
    def mock_registry(self):
        registry = MagicMock()
        registry.list_entities.return_value = ["Customers", "Transactions"]
        return registry

    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        spark.sparkContext.parallelize.return_value = MagicMock()
        spark.createDataFrame.return_value = MagicMock()
        return spark

    def test_ingestion_instantiation(self, mock_spark, mock_registry):
        ingester = BronzeIngestion(mock_spark, mock_registry, "bank_bronze", "my-bucket")
        assert ingester.bq_dataset == "bank_bronze"
        assert ingester.gcs_temp_bucket == "my-bucket"

    def test_ingest_file_calls_parser(self, mock_spark, mock_registry, tmp_path):
        content = "[ENTITY:Customers]\nCustomerID,Name\n1,Alice\n"
        f = tmp_path / "day0.txt"
        f.write_text(content)

        ingester = BronzeIngestion(mock_spark, mock_registry, "bank_bronze", "bucket")

        with patch.object(ingester, "_write_with_replay_safety", return_value=1) as mock_write:
            stats = ingester.ingest_file(str(f), "BATCH_001", "2024-01-01")
            assert "Customers" in stats
            mock_write.assert_called_once()

    def test_empty_entity_skipped(self, mock_spark, mock_registry, tmp_path):
        content = "[ENTITY:Customers]\nCustomerID,Name\n"
        f = tmp_path / "day0.txt"
        f.write_text(content)

        ingester = BronzeIngestion(mock_spark, mock_registry, "bank_bronze", "bucket")
        with patch.object(ingester, "_write_with_replay_safety", return_value=0) as mock_write:
            stats = ingester.ingest_file(str(f), "BATCH_001")
            assert stats.get("Customers", 0) == 0
            mock_write.assert_not_called()
