"""Model evaluation helpers and elbow/silhouette plot generation."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def compute_wssse(model) -> float:
    """Return Spark MLlib K-Means within-cluster sum of squared errors."""
    return float(model.summary.trainingCost)


def compute_silhouette(predictions, features_col: str = "scaled_features",
                       prediction_col: str = "prediction") -> float:
    """Calculate squared-Euclidean silhouette using Spark MLlib."""
    from pyspark.ml.evaluation import ClusteringEvaluator
    evaluator = ClusteringEvaluator(featuresCol=features_col, predictionCol=prediction_col,
                                     metricName="silhouette", distanceMeasure="squaredEuclidean")
    return float(evaluator.evaluate(predictions))


def plot_metrics(records: list[dict], output: str | Path) -> Path:
    """Save WSSSE and silhouette versus k as a two-panel PNG."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    records = sorted(records, key=lambda row: int(row["k"]))
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    ks = [int(row["k"]) for row in records]
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    axes[0].plot(ks, [float(row["wssse"]) for row in records], marker="o")
    axes[0].set(title="K-Means elbow", xlabel="k", ylabel="WSSSE")
    axes[1].plot(ks, [float(row["silhouette"]) for row in records], marker="o", color="#d95f02")
    axes[1].set(title="Silhouette", xlabel="k", ylabel="Score")
    fig.tight_layout()
    fig.savefig(destination, dpi=160)
    plt.close(fig)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/processed/model_metrics.json")
    parser.add_argument("--output", default="data/processed/plots/kmeans_evaluation.png")
    args = parser.parse_args()
    with Path(args.input).open(encoding="utf-8") as stream:
        records = json.load(stream)
    if isinstance(records, dict):
        records = records["metrics"]
    print(plot_metrics(records, args.output))


if __name__ == "__main__":
    main()
