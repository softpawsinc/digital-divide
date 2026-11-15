"""Aggregate tile-level measures by region and join socioeconomic features."""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession, functions as F

from src.config import load_config, resolve_path
from src.regions import get_region, hdfs_path

LOG = logging.getLogger("feature_engineering")
CONNECTIVITY_COLUMNS = ["avg_download_mbps", "avg_upload_mbps", "avg_latency_ms"]
GROUP_COLUMNS = ["region_id", "parent_1", "parent_2", "region_name"]


def aggregate_regions(tiles, socio, socio_id: str = "region_id",
                      socio_features: list[str] | None = None):
    """Create test-weighted connectivity metrics per region and attach socio data."""
    tests = F.sum("test_count")
    group_columns = [name for name in GROUP_COLUMNS if name in tiles.columns]
    aggregates = (tiles.groupBy(*group_columns)
                  .agg((F.sum(F.col("avg_download_mbps") * F.col("test_count")) / tests)
                       .alias("avg_download_mbps"),
                       (F.sum(F.col("avg_upload_mbps") * F.col("test_count")) / tests)
                       .alias("avg_upload_mbps"),
                       (F.sum(F.col("avg_latency_ms") * F.col("test_count")) / tests)
                       .alias("avg_latency_ms"),
                       tests.alias("test_count"), F.sum("device_count").alias("device_count_sum")))
    if socio_id != "region_id" and socio_id in socio.columns:
        socio = socio.withColumnRenamed(socio_id, "region_id")
    features = socio_features or [name for name in socio.columns
                                  if name not in ("region_id", "region_name", "parent_1",
                                                  "parent_2", "updated_at")]
    selected = socio.select("region_id", *[F.col(name).cast("double") for name in features])
    return aggregates.join(selected, "region_id", "left")


def filter_and_impute(frame, feature_columns: list[str], min_observed: int = 3):
    """Drop regions missing too many model features, then median-impute."""
    nonmissing = sum(F.when(F.col(column).isNotNull(), 1).otherwise(0)
                     for column in feature_columns)
    frame = frame.withColumn("_observed_feature_count", nonmissing) \
                 .filter(F.col("_observed_feature_count") >= min_observed)
    for column in feature_columns:
        values = frame.approxQuantile(column, [0.5], 0.01)
        median = float(values[0]) if values else 0.0
        frame = frame.withColumn(column, F.coalesce(F.col(column), F.lit(median)))
    return frame.drop("_observed_feature_count")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="us")
    parser.add_argument("--tiles")
    parser.add_argument("--socio")
    parser.add_argument("--output")
    parser.add_argument("--config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    region = get_region(cfg, args.region)
    tiles_path = args.tiles or hdfs_path(cfg, region, "joined")
    socio_path = args.socio or f"file://{resolve_path(region['socio'])}"
    output = args.output or hdfs_path(cfg, region, "features")
    feature_columns = list(region["model_features"])
    spark = SparkSession.builder.appName(f"DigitalDivide-Features-{args.region}").getOrCreate()
    try:
        tiles = spark.read.parquet(tiles_path)
        socio = spark.read.parquet(socio_path)
        features = filter_and_impute(
            aggregate_regions(tiles, socio, region.get("socio_id", "region_id"),
                              region.get("socio_features")),
            feature_columns)
        LOG.info("Feature rows for region %s: %d", args.region, features.count())
        features.write.mode("overwrite").parquet(output)
        LOG.info("Wrote region features to %s", output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
