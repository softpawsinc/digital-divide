"""Spark and spatial smoke tests for tile normalization and tract matching."""
import pytest
from pyspark.sql import SparkSession
from shapely.geometry import box

from src.process.clean_tiles import clean_dataframe, normalize_tiles
from src.process.geo_join import make_region_udf


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("digital-divide-tests")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_clean_tiles_converts_kbps_and_filters_invalid_rows(spark):
    raw = spark.createDataFrame([
        ("POINT (-122.0 37.0)", 25000, 5000, 20, 3, 2),
        ("POINT (-122.0 37.0)", -1, 5000, 20, 3, 2),
    ], ["tile", "avg_d_kbps", "avg_u_kbps", "avg_lat_ms", "tests", "devices"])
    normalized = normalize_tiles(raw)
    rows = clean_dataframe(normalized).collect()
    assert len(rows) == 1
    assert rows[0].avg_download_mbps == 25
    assert rows[0].avg_upload_mbps == 5
    assert rows[0].latitude == 37
    assert rows[0].longitude == -122


def test_spatial_udf_assigns_points_to_region(spark):
    polygon = box(-123, 36, -121, 38)
    attributes = {"parent_1": "06", "parent_2": "001", "region_name": "Example Tract"}
    udf = make_region_udf(spark, [("06001000100", attributes, bytes(polygon.wkb))])
    points = spark.createDataFrame([(37.0, -122.0), (40.0, -122.0)], ["latitude", "longitude"])
    found = [row.feature.region_id if row.feature else None
             for row in points.withColumn("feature", udf("latitude", "longitude")).collect()]
    assert found == ["06001000100", None]
