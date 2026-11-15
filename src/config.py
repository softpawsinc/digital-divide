"""Project configuration with YAML defaults and environment overrides."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"


def _set_path(data: dict[str, Any], dotted: str, value: Any) -> None:
    keys = dotted.split(".")
    node = data
    for key in keys[:-1]:
        node = node.setdefault(key, {})
    node[keys[-1]] = value


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load YAML configuration, then apply DIGITAL_DIVIDE_* overrides.

    Nested values use double underscores, e.g. DIGITAL_DIVIDE_MONGODB__URI.
    """
    config_path = Path(path or os.getenv("DIGITAL_DIVIDE_CONFIG", DEFAULT_CONFIG))
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path
    with config_path.open(encoding="utf-8") as stream:
        config = yaml.safe_load(stream) or {}

    for name, raw in os.environ.items():
        prefix = "DIGITAL_DIVIDE_"
        if not name.startswith(prefix):
            continue
        key = name[len(prefix):].lower().replace("__", ".")
        value: Any = raw
        if raw.lower() in {"true", "false"}:
            value = raw.lower() == "true"
        else:
            try:
                value = int(raw)
            except ValueError:
                try:
                    value = float(raw)
                except ValueError:
                    pass
        _set_path(config, key, value)

    # Convenient legacy overrides are accepted as well.
    aliases = {
        "HDFS_NAMENODE": "hdfs.namenode",
        "MONGODB_URI": "mongodb.uri",
        "MONGODB_DATABASE": "mongodb.database",
        "MONGODB_COLLECTION": "mongodb.collection",
        "CENSUS_API_KEY": "census.api_key",
        "CENSUS_YEAR": "census.year",
        "OOKLA_TYPE": "ookla.type",
        "OOKLA_YEAR": "ookla.year",
        "OOKLA_QUARTER": "ookla.quarter",
        "HDFS_BASE_PATH": "hdfs.base_path",
    }
    for env_name, dotted in aliases.items():
        if os.getenv(env_name):
            _set_path(config, dotted, os.environ[env_name])
    return config


def resolve_path(path: str | Path) -> Path:
    """Resolve a configured relative path from the project root."""
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate
