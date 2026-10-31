"""
Metadata registry — single source of truth for entity configs and validation rules.

Loads entities.yaml and validation_rules.yaml once; all pipeline layers query
this registry instead of hardcoding entity-specific logic.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


_DEFAULT_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


class MetadataRegistry:
    """
    Provides entity configuration and validation-rule lookups driven by YAML files.

    Adding a new entity requires only:
      1. A new entry in entities.yaml
      2. A matching JSON schema under config/schemas/
      3. Any relevant rules in validation_rules.yaml
    No core pipeline code changes are needed.
    """

    def __init__(self, config_dir: Optional[str] = None):
        self._config_dir = Path(config_dir) if config_dir else _DEFAULT_CONFIG_DIR
        self._entities: Dict[str, Any] = {}
        self._rules: List[Dict[str, Any]] = []
        self._schemas: Dict[str, Any] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_entities(self) -> List[str]:
        """Return all entity names defined in entities.yaml."""
        return list(self._entities.keys())

    def get_entity_config(self, entity_name: str) -> Dict[str, Any]:
        """Return full config block for *entity_name*."""
        if entity_name not in self._entities:
            raise KeyError(f"Unknown entity: {entity_name!r}. Known: {self.list_entities()}")
        return self._entities[entity_name]

    def get_primary_key(self, entity_name: str) -> str:
        return self.get_entity_config(entity_name)["primary_key"]

    def get_required_fields(self, entity_name: str) -> List[str]:
        return self.get_entity_config(entity_name).get("required_fields", [])

    def get_scd_type(self, entity_name: str) -> int:
        return int(self.get_entity_config(entity_name).get("scd_type", 1))

    def get_scd_track_columns(self, entity_name: str) -> List[str]:
        return self.get_entity_config(entity_name).get("scd_track_columns", [])

    def get_target_table(self, entity_name: str) -> str:
        return self.get_entity_config(entity_name).get(
            "target_table", entity_name.lower()
        )

    def get_schema(self, entity_name: str) -> Optional[Dict[str, Any]]:
        """Return JSON schema definition for *entity_name*, or None if not found."""
        return self._schemas.get(entity_name)

    def get_field_types(self, entity_name: str) -> Dict[str, str]:
        """Return dict of field_name → BigQuery type string for *entity_name*."""
        schema = self.get_schema(entity_name)
        if schema is None:
            return {}
        return {f["name"]: f["type"] for f in schema.get("fields", [])}

    def get_rules_for_entity(self, entity_name: str) -> List[Dict[str, Any]]:
        """Return all validation rules that apply to *entity_name*."""
        return [r for r in self._rules if r.get("entity") == entity_name]

    def get_rule_by_id(self, rule_id: str) -> Optional[Dict[str, Any]]:
        for rule in self._rules:
            if rule.get("rule_id") == rule_id:
                return rule
        return None

    def all_rules(self) -> List[Dict[str, Any]]:
        return list(self._rules)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> None:
        entities_path = self._config_dir / "entities.yaml"
        rules_path = self._config_dir / "validation_rules.yaml"
        schemas_dir = self._config_dir / "schemas"

        with open(entities_path) as f:
            data = yaml.safe_load(f)
            self._entities = data.get("entities", {})

        with open(rules_path) as f:
            data = yaml.safe_load(f)
            self._rules = data.get("rules", [])

        if schemas_dir.exists():
            for schema_file in schemas_dir.glob("*.json"):
                with open(schema_file) as f:
                    schema = json.load(f)
                    entity_name = schema.get("entity")
                    if entity_name:
                        self._schemas[entity_name] = schema

    def __repr__(self) -> str:
        return (
            f"MetadataRegistry(entities={self.list_entities()}, "
            f"rules={len(self._rules)})"
        )
