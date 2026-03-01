# 1
import json
import yaml
from pathlib import Path
from typing import Any

class Config:
    """
    Load schema.json and rules.yaml once
    and expose them to the application.
    """
    def __init__(self, schema_path: Path, rules_path: Path) -> None:
        self.schema = self._load_json(schema_path)
        self.rules = self._load_yaml(rules_path)

    def _load_json(self, path: Path) -> dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load JSON config: {path}") from e
    
    def _load_yaml(self, path: Path) -> dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            raise RuntimeError(f"Failed to load YAML config: {path}") from e