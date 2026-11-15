"""Join MongoDB cluster labels to region boundaries and export a GeoJSON map."""
from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pymongo import MongoClient

from src.config import load_config, resolve_path
from src.regions import get_region

PROPERTY_COLUMNS = ["region_id", "region_name", "parent_1", "parent_2",
                    "cluster_id", "cluster_label"]


def export_geojson(boundaries: str | Path, output: str | Path, uri: str,
                   database: str, collection: str, id_column: str = "GEOID",
                   id_width: int = 0, simplify_tolerance: float = 0.001) -> Path:
    """Write only regions present in the latest cluster collection."""
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        target = client[database][collection]
        latest = target.find_one({"region_id": {"$exists": True}}, {"updated_at": 1},
                                 sort=[("updated_at", -1)])
        if not latest:
            raise ValueError("MongoDB has no cluster records; run the pipeline first")
        updated_at = latest["updated_at"]
        labels = list(target.find({"updated_at": updated_at}, {"_id": 0}))
    finally:
        client.close()
    if not labels:
        raise ValueError("No cluster documents found for the latest pipeline run")
    shapes = gpd.read_file(boundaries)
    if id_column not in shapes.columns:
        if {"STATEFP", "COUNTYFP", "TRACTCE"}.issubset(shapes.columns):
            shapes["GEOID"] = (shapes["STATEFP"].astype(str).str.zfill(2)
                               + shapes["COUNTYFP"].astype(str).str.zfill(3)
                               + shapes["TRACTCE"].astype(str).str.zfill(6))
            id_column = "GEOID"
        else:
            raise ValueError(f"Boundary file lacks id column {id_column}")
    shapes["region_id"] = shapes[id_column].astype(str)
    if id_width:
        shapes["region_id"] = shapes["region_id"].str.zfill(id_width)
    attrs = pd.DataFrame(labels)
    if "updated_at" in attrs.columns:
        attrs["updated_at"] = pd.to_datetime(attrs["updated_at"], utc=True).astype(str)
    merged = shapes.merge(attrs, on="region_id", how="inner")
    if merged.empty:
        raise ValueError("No boundary shapes matched the latest cluster labels")
    keep = [column for column in attrs.columns if column in merged.columns]
    properties = [column for column in PROPERTY_COLUMNS if column in merged.columns]
    extras = [column for column in keep if column not in properties and column != "region_id"]
    merged = merged[properties + extras + ["geometry"]]
    if simplify_tolerance and simplify_tolerance > 0:
        merged["geometry"] = merged.geometry.simplify(simplify_tolerance, preserve_topology=True)
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    merged.to_file(destination, driver="GeoJSON", index=False)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="us")
    parser.add_argument("--boundaries")
    parser.add_argument("--output")
    parser.add_argument("--simplify-tolerance", type=float,
                        help="Simplification tolerance in degrees (0 disables)")
    parser.add_argument("--config")
    args = parser.parse_args()
    cfg = load_config(args.config)
    region = get_region(cfg, args.region)
    boundary_path = Path(args.boundaries) if args.boundaries else Path(resolve_path(region["boundary"]))
    if boundary_path.is_dir():
        boundary_path = next(boundary_path.glob("*.shp"), None)
    if not boundary_path:
        raise FileNotFoundError(f"No boundary file found for region {args.region}")
    tolerance = args.simplify_tolerance if args.simplify_tolerance is not None \
        else float(region.get("simplify_tolerance", 0.0))
    result = export_geojson(boundary_path, resolve_path(args.output or region["geojson"]),
                            cfg["mongodb"]["uri"], cfg["mongodb"]["database"],
                            region.get("mongo_collection", cfg["mongodb"]["collection"]),
                            region.get("id_column", "GEOID"), int(region.get("id_width", 0)),
                            tolerance)
    print(result)


if __name__ == "__main__":
    main()
