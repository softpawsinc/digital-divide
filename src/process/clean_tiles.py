"""Normalize and quality-filter Ookla tile aggregates with Spark."""
from __future__ import annotations

import argparse
import logging
import math
from typing import Any

from pyspark.sql import SparkSession, functions as F, types as T

from src.config import load_config
from src.regions import get_region, hdfs_path, region_bbox

LOG = logging.getLogger("clean_tiles")


def _tile_center(tile: Any) -> tuple[float | None, float | None]:
    """Return (latitude, longitude) from Ookla's WKT/WKB tile geometry."""
    if tile is None:
        return None, None
    try:
        from shapely import wkb, wkt
        if isinstance(tile, (bytes, bytearray, memoryview)):
            geom = wkb.loads(bytes(tile))
        else:
            raw = str(tile).strip()
            try:
                geom = wkt.loads(raw)
            except Exception:
                geom = wkb.loads(bytes.fromhex(raw.removeprefix("0x")))
        center = geom.centroid
        if center.is_empty or not math.isfinite(center.x) or not math.isfinite(center.y):
            return None, None
        return float(center.y), float(center.x)
    except Exception:
        return None, None


def normalize_tiles(df):
    """Rename source variants, cast metrics, and add latitude/longitude."""
    names = set(df.columns)
    for source, target in [("avg_d_kbps", "avg_download_mbps"), ("avg_u_kbps", "avg_upload_mbps")]:
        if source in names and target not in names:
            df = df.withColumn(target, F.col(source).cast("double") / 1000.0)
    for source, target in [("avg_lat_ms", "avg_latency_ms"), ("tests", "test_count"),
                           ("devices", "device_count")]:
        if source in names and target not in names:
            df = df.withColumnRenamed(source, target)
    for name in ("avg_download_mbps", "avg_upload_mbps", "avg_latency_ms", "test_count", "device_count"):
        if name not in df.columns:
            raise ValueError(f"Ookla input is missing required metric column: {name}")
        df = df.withColumn(name, F.col(name).cast("double"))

    if "lat" in df.columns and "lon" in df.columns:
        df = df.withColumn("latitude", F.col("lat").cast("double")) \
               .withColumn("longitude", F.col("lon").cast("double"))
    elif "tile_y" in df.columns and "tile_x" in df.columns:
        df = df.withColumn("latitude", F.col("tile_y").cast("double")) \
               .withColumn("longitude", F.col("tile_x").cast("double"))
    elif "tile" in df.columns:
        center_schema = T.StructType([T.StructField("latitude", T.DoubleType()),
                                      T.StructField("longitude", T.DoubleType())])
        center_udf = F.udf(_tile_center, center_schema)
        df = df.withColumn("_center", center_udf("tile")) \
               .withColumn("latitude", F.col("_center.latitude")) \
               .withColumn("longitude", F.col("_center.longitude")).drop("_center")
    else:
        raise ValueError("Ookla input has no coordinates, tile_x/tile_y, or tile geometry")
    return df


def clean_dataframe(df, min_tests: int = 1, max_latency_ms: float = 1000,
                    bbox: list[float] | None = None):
    """Keep records with valid coordinates and plausible positive measurements."""
    valid = (
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
        & F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180)
        & F.col("avg_download_mbps").isNotNull() & (F.col("avg_download_mbps") >= 0)
        & F.col("avg_upload_mbps").isNotNull() & (F.col("avg_upload_mbps") >= 0)
        & F.col("avg_latency_ms").isNotNull() & (F.col("avg_latency_ms") >= 0)
        & (F.col("avg_latency_ms") <= max_latency_ms)
        & F.col("test_count").isNotNull() & (F.col("test_count") >= min_tests)
    )
    if bbox is not None:
        min_lat, max_lat, min_lon, max_lon = (float(value) for value in bbox)
        valid = valid & F.col("latitude").between(min_lat, max_lat) \
                      & F.col("longitude").between(min_lon, max_lon)
    return df.filter(valid)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="us")
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("MIN_LAT", "MAX_LAT", "MIN_LON", "MAX_LON"))
    parser.add_argument("--config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    region = get_region(cfg, args.region)
    base = cfg["hdfs"]["base_path"].rstrip("/")
    ookla = cfg["ookla"]
    source = args.input or (f"{base}/raw/ookla/type={ookla['type']}/year={ookla['year']}"
                            f"/quarter={ookla['quarter']}")
    destination = args.output or hdfs_path(cfg, region, "clean_tiles")
    spark = (SparkSession.builder.appName(f"DigitalDivide-CleanTiles-{args.region}")
             .config("spark.sql.shuffle.partitions", str(cfg["spark"].get("shuffle_partitions", 200)))
             .getOrCreate())
    try:
        raw = spark.read.parquet(source)
        bbox = args.bbox if args.bbox is not None else region_bbox(cfg, region)
        cleaned = clean_dataframe(normalize_tiles(raw), int(cfg["ookla"].get("min_tests", 1)),
                                   float(cfg["ookla"].get("max_latency_ms", 1000)), bbox)
        LOG.info("Input rows=%s; accepted rows=%s%s", raw.count(), cleaned.count(),
                 f" (bbox={bbox})" if bbox else "")
        cleaned.write.mode("overwrite").parquet(destination)
        LOG.info("Wrote cleaned tiles to %s", destination)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
