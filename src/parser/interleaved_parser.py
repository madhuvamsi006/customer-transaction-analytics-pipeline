"""
Generic parser for interleaved multi-entity flat files.

File format:
  [ENTITY:EntityName]
  col1,col2,...
  row1val1,row1val2,...
  ...
  [ENTITY:NextEntity]
  ...

Entity blocks can appear in any order. The parser is config-driven and
returns a dict mapping entity name → list of row dicts.
"""

from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Dict, List, Optional


ENTITY_HEADER_PATTERN = re.compile(r"^\[ENTITY:(\w+)\]$")


class ParsedEntity:
    """Container for one entity block extracted from an interleaved file."""

    def __init__(self, name: str, columns: List[str], rows: List[Dict[str, str]]):
        self.name = name
        self.columns = columns
        self.rows = rows

    def __repr__(self) -> str:
        return f"ParsedEntity(name={self.name!r}, rows={len(self.rows)})"


class InterleavedFileParser:
    """
    Reads a flat file containing multiple interleaved entity blocks delimited
    by [ENTITY:Name] markers. Does not assume a fixed entity order.

    Usage:
        parser = InterleavedFileParser()
        result = parser.parse("/path/to/day0.txt")
        customers = result["Customers"]   # ParsedEntity
        for row in customers.rows:
            print(row["CustomerID"], row["Email"])
    """

    def parse(self, file_path: str, encoding: str = "utf-8") -> Dict[str, ParsedEntity]:
        """Parse an interleaved file and return a dict of entity name → ParsedEntity."""
        content = Path(file_path).read_text(encoding=encoding)
        return self.parse_text(content)

    def parse_text(self, content: str) -> Dict[str, ParsedEntity]:
        """Parse raw text content of an interleaved file."""
        entities: Dict[str, ParsedEntity] = {}
        current_entity: Optional[str] = None
        current_columns: List[str] = []
        current_rows: List[Dict[str, str]] = []
        header_seen = False

        for raw_line in content.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            match = ENTITY_HEADER_PATTERN.match(line)
            if match:
                # Save previous entity before starting a new one
                if current_entity is not None:
                    entities[current_entity] = ParsedEntity(
                        current_entity, current_columns, current_rows
                    )
                current_entity = match.group(1)
                current_columns = []
                current_rows = []
                header_seen = False
                continue

            if current_entity is None:
                # Lines before any entity marker are ignored
                continue

            if not header_seen:
                # First non-blank line after entity marker is the CSV header
                current_columns = [c.strip() for c in line.split(",")]
                header_seen = True
                continue

            # Data row — use csv.reader to handle quoted fields correctly
            reader = csv.reader(io.StringIO(line))
            values = next(reader, [])
            # Pad or truncate to match column count
            if len(values) < len(current_columns):
                values += [""] * (len(current_columns) - len(values))
            row = {col: val.strip() for col, val in zip(current_columns, values)}
            current_rows.append(row)

        # Flush last entity
        if current_entity is not None:
            entities[current_entity] = ParsedEntity(
                current_entity, current_columns, current_rows
            )

        return entities

    def parse_to_dataframe(self, file_path: str):
        """
        Parse file and return dict of entity name → pandas DataFrame.
        Requires pandas; used for local testing / Silver layer when not on Spark.
        """
        import pandas as pd

        result = self.parse(file_path)
        return {
            name: pd.DataFrame(entity.rows, columns=entity.columns)
            for name, entity in result.items()
        }
