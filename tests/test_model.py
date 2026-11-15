"""Synthetic data test for Spark ML K-Means training and segment output."""
import pytest
from pyspark.sql import SparkSession

from src.model.kmeans_clustering import train


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("digital-divide-model-tests")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_kmeans_produces_two_clusters(spark):
    rows = []
    for index in range(12):
        underserved = index < 6
        rows.append((f"0600100{index:04d}", "06", "001",
                     15.0 if underserved else 150.0,
                     5.0 if underserved else 30.0,
                     80.0 if underserved else 20.0,
                     45.0 if underserved else 95.0,
                     30000.0 if underserved else 90000.0,
                     20.0 if underserved else 60.0))
    columns = ["tract_id", "state", "county", "avg_download_mbps", "avg_upload_mbps",
               "avg_latency_ms", "broadband_adoption_pct", "median_income",
               "education_bachelors_pct"]
    frame = spark.createDataFrame(rows, columns)
    output, scaler, model, metrics, selected_k = train(frame, k_min=2, k_max=2, silhouette_sample_size=100)
    assert selected_k == 2
    assert output.select("cluster_id").distinct().count() == 2
    assert output.select("cluster_label").distinct().count() >= 1
    assert len(metrics) == 1 and metrics[0]["silhouette"] > 0
