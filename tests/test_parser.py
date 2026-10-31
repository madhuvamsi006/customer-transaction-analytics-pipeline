"""Unit tests for the interleaved file parser."""

import pytest
from src.parser.interleaved_parser import InterleavedFileParser, ParsedEntity


SAMPLE_CONTENT = """
[ENTITY:Customers]
CustomerID,FirstName,LastName,Email
1,Alice,Johnson,alice@test.com
2,Bob,Smith,bob@test.com

[ENTITY:Accounts]
AccountID,CustomerID,Balance
101,1,25000.00
102,2,8500.00
"""

SINGLE_ENTITY = """
[ENTITY:Branches]
BranchID,BranchName,SortCode
BR001,Downtown,001001
"""

MISSING_FIELD = """
[ENTITY:Customers]
CustomerID,FirstName,LastName,Email
3,Carol,Williams,
"""


class TestInterleavedFileParser:
    def setup_method(self):
        self.parser = InterleavedFileParser()

    def test_parse_multiple_entities(self):
        result = self.parser.parse_text(SAMPLE_CONTENT)
        assert "Customers" in result
        assert "Accounts" in result

    def test_customer_row_count(self):
        result = self.parser.parse_text(SAMPLE_CONTENT)
        assert len(result["Customers"].rows) == 2

    def test_account_row_count(self):
        result = self.parser.parse_text(SAMPLE_CONTENT)
        assert len(result["Accounts"].rows) == 2

    def test_field_values(self):
        result = self.parser.parse_text(SAMPLE_CONTENT)
        first = result["Customers"].rows[0]
        assert first["CustomerID"] == "1"
        assert first["FirstName"] == "Alice"
        assert first["Email"] == "alice@test.com"

    def test_entity_order_independent(self):
        # Reverse entity order — result must be same
        reversed_content = """
[ENTITY:Accounts]
AccountID,CustomerID,Balance
101,1,25000.00

[ENTITY:Customers]
CustomerID,FirstName,LastName,Email
1,Alice,Johnson,alice@test.com
"""
        result = self.parser.parse_text(reversed_content)
        assert "Customers" in result
        assert "Accounts" in result
        assert result["Customers"].rows[0]["CustomerID"] == "1"

    def test_single_entity(self):
        result = self.parser.parse_text(SINGLE_ENTITY)
        assert len(result) == 1
        assert "Branches" in result
        assert result["Branches"].rows[0]["SortCode"] == "001001"

    def test_missing_email_field_is_empty_string(self):
        result = self.parser.parse_text(MISSING_FIELD)
        row = result["Customers"].rows[0]
        assert row["Email"] == ""

    def test_columns_attribute(self):
        result = self.parser.parse_text(SAMPLE_CONTENT)
        entity = result["Customers"]
        assert isinstance(entity, ParsedEntity)
        assert "CustomerID" in entity.columns
        assert "Email" in entity.columns

    def test_empty_content(self):
        result = self.parser.parse_text("")
        assert result == {}

    def test_content_before_entity_marker_ignored(self):
        content = "some junk line\n" + SAMPLE_CONTENT
        result = self.parser.parse_text(content)
        assert "Customers" in result

    def test_parse_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text(SAMPLE_CONTENT)
        result = self.parser.parse(str(f))
        assert "Customers" in result
