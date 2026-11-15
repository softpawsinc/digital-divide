"""Region registry helpers for the shared multi-tier pipeline schema."""
from __future__ import annotations

from typing import Any


def get_region(cfg: dict[str, Any], name: str) -> dict[str, Any]:
    """Return the configuration block for one geographic tier."""
    regions = cfg.get("regions") or {}
    if name not in regions:
        raise SystemExit(f"Unknown region '{name}'; available regions: {sorted(regions)}")
    region = dict(regions[name])
    region.setdefault("label", name)
    return region


def hdfs_path(cfg: dict[str, Any], region: dict[str, Any], key: str) -> str:
    """Resolve one HDFS path for a region, e.g. key='joined'."""
    base = cfg["hdfs"]["base_path"].rstrip("/")
    relative = region["hdfs"][key].lstrip("/")
    return f"{base}/{relative}"


def region_bbox(cfg: dict[str, Any], region: dict[str, Any]) -> list[float] | None:
    """Region bounding box, falling back to the shared Ookla bbox for the US."""
    bbox = region.get("bbox")
    if bbox is None and region.get("level") == "tract":
        bbox = cfg.get("ookla", {}).get("bbox")
    return bbox
