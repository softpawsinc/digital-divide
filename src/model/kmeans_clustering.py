"""Train and evaluate Spark MLlib K-Means models for regional segments."""
from __future__ import annotations

import argparse
import json
import logging
from collections import Counter

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F

from src.config import load_config, resolve_path
from src.model.evaluate import compute_silhouette, compute_wssse, plot_metrics
from src.regions import get_region, hdfs_path

LOG = logging.getLogger("kmeans_clustering")
DEFAULT_FEATURE_COLUMNS = ["avg_download_mbps", "avg_upload_mbps", "avg_latency_ms",
                           "broadband_adoption_pct", "median_income", "education_bachelors_pct"]


def _segment_names(cluster_summary, adoption_median: float, speed_median: float,
                   income_median: float, adoption_feature: str, income_feature: str,
                   ) -> dict[int, str]:
    names = {}
    for row in cluster_summary:
        cluster = int(row["prediction"])
        speed = float(row["avg_download_mbps"] or 0)
        low_speed = speed < speed_median
        adoption = float(row[adoption_feature] or 0) if adoption_feature else None
        low_adoption = adoption is not None and adoption < adoption_median
        income = float(row[income_feature] or 0) if income_feature else None
        low_income = income is not None and income < income_median
        if adoption_feature is None:
            label = "underserved" if low_speed else "well-served"
        elif low_adoption and low_income:
            label = "low-income, low-adoption"
        elif low_adoption and low_speed:
            label = "underserved"
        elif low_adoption != low_speed:
            label = "mixed-access"
        else:
            label = "well-served"
        names[cluster] = label
    # Keep segments selectable when several clusters share the same rule label.
    repeated = Counter(names.values())
    return {cluster: f"{label} ({cluster})" if repeated[label] > 1 else label
            for cluster, label in names.items()}


def train(features, k_min: int = 3, k_max: int = 8, silhouette_sample_size: int = 20000,
          feature_columns: list[str] | None = None, adoption_feature: str | None = "broadband_adoption_pct",
          income_feature: str | None = "median_income", id_column: str = "region_id",
          label: str = "regions"):
    """Fit scaling once, compare k by sampled silhouette, and return final output."""
    feature_columns = feature_columns or DEFAULT_FEATURE_COLUMNS
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="raw_features",
                                handleInvalid="skip")
    assembled = assembler.transform(features)
    scaler = StandardScaler(inputCol="raw_features", outputCol="scaled_features",
                            withMean=True, withStd=True).fit(assembled)
    scaled = scaler.transform(assembled).cache()
    count = scaled.count()
    if count < 3:
        raise ValueError(f"At least three valid {label} are required for clustering")
    candidate_ks = list(range(max(2, k_min), min(k_max, count - 1) + 1))
    if not candidate_ks:
        raise ValueError("No valid k values; check model.k_min/model.k_max and feature row count")

    evaluation_data = scaled
    if count > silhouette_sample_size and id_column in scaled.columns:
        evaluation_data = scaled.orderBy(F.xxhash64(id_column)).limit(silhouette_sample_size).cache()
    metrics = []
    final_model = None
    best_score = float("-inf")
    for k in candidate_ks:
        estimator = KMeans(k=k, seed=42, featuresCol="scaled_features", predictionCol="prediction",
                           maxIter=50, tol=1e-4)
        model = estimator.fit(scaled)
        predicted = model.transform(evaluation_data)
        score = compute_silhouette(predicted)
        record = {"k": k, "wssse": compute_wssse(model), "silhouette": score}
        metrics.append(record)
        LOG.info("k=%d WSSSE=%.3f silhouette=%.5f", k, record["wssse"], score)
        if score > best_score:
            best_score, final_model = score, model
    if final_model is None:
        raise ValueError("K-Means training failed for every candidate k")
    # The chosen candidate already fit the complete scaled feature table.
    predictions = final_model.transform(scaled)
    summary_rows = (predictions.groupBy("prediction")
                    .agg(*[F.avg(column).alias(column) for column in feature_columns])
                    .collect())
    adoption_feature = adoption_feature if adoption_feature in feature_columns else None
    income_feature = income_feature if income_feature in feature_columns else None
    medians = {column: features.approxQuantile(column, [0.5], 0.01)[0]
               for column in feature_columns}
    labels = _segment_names(summary_rows,
                            medians.get(adoption_feature, 0.0) if adoption_feature else 0.0,
                            medians.get("avg_download_mbps", 0.0),
                            medians.get(income_feature, 0.0) if income_feature else 0.0,
                            adoption_feature, income_feature)
    label_expr = F.create_map(*[item for pair in labels.items()
                                for item in (F.lit(pair[0]), F.lit(pair[1]))])
    output = (predictions.withColumn("cluster_id", F.col("prediction").cast("int"))
              .withColumn("cluster_label", label_expr[F.col("cluster_id")])
              .drop("raw_features", "scaled_features", "prediction"))
    selected_k = max(candidate_ks, key=lambda k: next(
        m["silhouette"] for m in metrics if m["k"] == k))
    return output, scaler, final_model, metrics, selected_k


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="us")
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--model-output")
    parser.add_argument("--config")
    parser.add_argument("--plot-dir")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    region = get_region(cfg, args.region)
    base = cfg["hdfs"]["base_path"].rstrip("/")
    input_path = args.input or hdfs_path(cfg, region, "features")
    output_path = args.output or hdfs_path(cfg, region, "clusters")
    model_path = args.model_output or f"{base}/models/kmeans_{args.region}"
    spark = (SparkSession.builder.appName(f"DigitalDivide-KMeans-{args.region}")
             .config("spark.sql.shuffle.partitions", str(cfg["spark"].get("shuffle_partitions", 200)))
             .getOrCreate())
    try:
        features = spark.read.parquet(input_path).cache()
        output, scaler, model, metrics, selected_k = train(
            features, int(cfg["model"].get("k_min", 3)), int(cfg["model"].get("k_max", 8)),
            int(cfg["model"].get("silhouette_sample_size", 20000)),
            feature_columns=list(region["model_features"]),
            adoption_feature=region.get("adoption_feature"),
            income_feature=region.get("income_feature"),
            label=region.get("level", "regions"))
        output.write.mode("overwrite").parquet(output_path)
        scaler.write().overwrite().save(f"{model_path}/scaler")
        model.write().overwrite().save(f"{model_path}/kmeans")
        artifact_dir = resolve_path("data/processed")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = resolve_path(region["metrics"])
        with metrics_path.open("w", encoding="utf-8") as stream:
            json.dump({"region": args.region, "selected_k": selected_k, "metrics": metrics},
                      stream, indent=2)
        plot_path = resolve_path(args.plot_dir or region["plot"].rsplit("/", 1)[0]) \
            / region["plot"].rsplit("/", 1)[1]
        plot_metrics(metrics, plot_path)
        LOG.info("Selected k=%d for region %s; wrote cluster labels to %s",
                 selected_k, args.region, output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
