"""Assign cleaned Ookla tile centroids to region polygons using Shapely STRtree."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import geopandas as gpd
from pyspark.sql import SparkSession, functions as F, types as T
from shapely import wkb
from shapely.strtree import STRtree

from src.config import load_config, resolve_path
from src.regions import get_region, hdfs_path

LOG = logging.getLogger("geo_join")
_TREE_CACHE = {}
JOIN_FIELDS = ("parent_1", "parent_2", "region_name")


def _text_column(shapes, source: str | None, expected: int) -> list[str | None]:
    """Extract one join attribute column as plain strings (None where missing)."""
    if not source or source not in shapes.columns:
        return [None] * expected
    values = []
    for value in shapes[source].tolist():
        values.append(None if value is None or value != value else str(value))
    return values


def read_region_records(path: str | Path, id_column: str | None = None,
                        id_width: int = 0, filter_column: str | None = None,
                        filter_values: list[str] | None = None,
                        join_fields: dict[str, str] | None = None,
                        ) -> list[tuple[str, dict[str, str | None], bytes]]:
    """Load region ids, join attributes, and WKB polygons in WGS84 for broadcast."""
    LOG.info("Reading boundary file %s", path)
    shapes = gpd.read_file(path)
    if shapes.crs is None:
        raise ValueError(f"Boundary file has no CRS: {path}")
    shapes = shapes.to_crs("EPSG:4326")
    if filter_column and filter_values:
        if filter_column not in shapes.columns:
            raise ValueError(f"Boundary file lacks filter column {filter_column}")
        wanted = {str(value) for value in filter_values}
        shapes = shapes[shapes[filter_column].astype(str).isin(wanted)]
        if shapes.empty:
            raise ValueError(f"No polygons match {filter_column}={sorted(wanted)}")
    id_column = id_column if id_column in shapes.columns else None
    if not id_column and {"STATEFP", "COUNTYFP", "TRACTCE"}.issubset(shapes.columns):
        shapes["GEOID"] = shapes["STATEFP"].astype(str).str.zfill(2) + \
                           shapes["COUNTYFP"].astype(str).str.zfill(3) + \
                           shapes["TRACTCE"].astype(str).str.zfill(6)
        id_column = "GEOID"
    if not id_column:
        raise ValueError(f"Boundary file must include the configured id column ({path})")
    LOG.info("Filtered %d polygons; extracting id and attribute columns", len(shapes))
    join_fields = join_fields or {}
    # Extract whole columns as plain Python lists; per-row pandas Series access
    # (iterrows) makes reading nationwide boundary files unbearably slow.
    ids = shapes[id_column].tolist()
    geometries = shapes.geometry.tolist()
    columns = {target: _text_column(shapes, join_fields.get(target), len(shapes))
               for target in JOIN_FIELDS}
    records = []
    for region_id, geom, *values in zip(ids, geometries, *columns.values()):
        if geom is None or geom.is_empty:
            continue
        if not geom.is_valid:
            geom = geom.make_valid()
        attributes = dict(zip(columns, values))
        records.append((str(region_id).zfill(id_width) if id_width else str(region_id),
                        attributes, bytes(geom.wkb)))
    if not records:
        raise ValueError(f"No usable polygons in {path}")
    return records


def _build_tree(records):
    geometries = [wkb.loads(blob) for _, _, blob in records]
    tree = STRtree(geometries)
    ids = [region_id for region_id, _, _ in records]
    index_of = {id(geom): index for index, geom in enumerate(geometries)}
    return tree, geometries, index_of, ids


def make_region_udf(spark, records):
    """Build a scalar Spark UDF backed by one lazily initialized STRtree per worker."""
    broadcast_records = spark.sparkContext.broadcast(records)
    attributes = [attrs for _, attrs, _ in records]
    schema = T.StructType([T.StructField(name, T.StringType()) for name in
                           ("region_id",) + JOIN_FIELDS])

    def find_region(latitude, longitude):
        if latitude is None or longitude is None:
            return None
        cache_key = id(broadcast_records)
        cached = _TREE_CACHE.get(cache_key)
        if cached is None:
            cached = _build_tree(broadcast_records.value)
            _TREE_CACHE[cache_key] = cached
        tree, geometries, index_of, ids = cached
        from shapely.geometry import Point
        point = Point(float(longitude), float(latitude))
        candidates = tree.query(point)
        for candidate in candidates:
            if hasattr(candidate, "geom_type"):
                index = index_of.get(id(candidate))
                geom = candidate
            else:
                index = int(candidate)
                geom = geometries[index]
            if index is None:
                continue
            if geom.covers(point):
                attrs = attributes[index]
                return (ids[index], attrs["parent_1"], attrs["parent_2"], attrs["region_name"])
        return None

    return F.udf(find_region, schema)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="us")
    parser.add_argument("--input")
    parser.add_argument("--boundaries")
    parser.add_argument("--output")
    parser.add_argument("--config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    region = get_region(cfg, args.region)
    source = args.input or hdfs_path(cfg, region, "clean_tiles")
    destination = args.output or hdfs_path(cfg, region, "joined")
    boundary_path = Path(args.boundaries) if args.boundaries else Path(resolve_path(region["boundary"]))
    if boundary_path.is_dir():
        boundary_path = next(boundary_path.glob("*.shp"), None)
    if not boundary_path or not boundary_path.exists():
        raise FileNotFoundError(f"Could not find boundary file for region {args.region}: {boundary_path}")
    boundary_filter = region.get("boundary_filter") or {}
    records = read_region_records(
        boundary_path, region.get("id_column"), int(region.get("id_width", 0)),
        boundary_filter.get("column"), boundary_filter.get("values"),
        region.get("join_fields"))
    LOG.info("Loaded %d %s polygons for region %s", len(records),
             region.get("level", "region"), args.region)
    spark = SparkSession.builder.appName(f"DigitalDivide-GeoJoin-{args.region}").getOrCreate()
    try:
        tiles = spark.read.parquet(source)
        joined = (tiles.withColumn("feature", make_region_udf(spark, records)
                                   ("latitude", "longitude"))
                  .filter(F.col("feature").isNotNull())
                  .select("*", "feature.*").drop("feature"))
        total, matched = tiles.count(), joined.count()
        LOG.info("Unmatched tile rows: %d of %d", total - matched, total)
        joined.write.mode("overwrite").parquet(destination)
        LOG.info("Wrote region-assigned tiles to %s", destination)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
