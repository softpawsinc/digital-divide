"""Upsert regional cluster results into MongoDB and expose dashboard query helpers."""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
import math

from pymongo import MongoClient, ReplaceOne
from pyspark.sql import SparkSession

from src.config import load_config
from src.regions import get_region, hdfs_path

LOG = logging.getLogger("mongo_writer")
LEGACY_INDEXES = ("tract_id_1", "state_1")


def get_collection(uri: str, database: str, collection: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    target = client[database][collection]
    for legacy in LEGACY_INDEXES:
        try:
            target.drop_index(legacy)
        except Exception:
            pass
    target.create_index("region_id", unique=True,
                        partialFilterExpression={"region_id": {"$exists": True}})
    target.create_index("parent_1")
    target.create_index("cluster_label")
    target.create_index("updated_at")
    return client, target


def write_spark_dataframe(dataframe, uri: str, database: str, collection: str,
                          batch_size: int = 1000, id_width: int = 0) -> int:
    """Stream Spark rows to Mongo in bounded upsert batches."""
    client, target = get_collection(uri, database, collection)
    now = datetime.now(timezone.utc)
    operations = []
    written = 0
    try:
        for row in dataframe.toLocalIterator():
            document = row.asDict(recursive=True)
            document.pop("_id", None)
            for key, value in list(document.items()):
                if isinstance(value, float) and not math.isfinite(value):
                    document[key] = None
            region_id = str(document["region_id"])
            document["region_id"] = region_id.zfill(id_width) if id_width else region_id
            document["updated_at"] = now
            operations.append(ReplaceOne({"region_id": document["region_id"]}, document,
                                         upsert=True))
            if len(operations) >= batch_size:
                target.bulk_write(operations, ordered=False)
                written += len(operations)
                operations.clear()
                LOG.info("Upserted %d region documents", written)
        if operations:
            target.bulk_write(operations, ordered=False)
            written += len(operations)
        return written
    finally:
        client.close()


def latest_run_id(uri: str, database: str, collection: str) -> str | None:
    """Return the newest ingest timestamp for dashboard filtering."""
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        doc = client[database][collection].find_one({}, {"updated_at": 1},
                                                    sort=[("updated_at", -1)])
        return doc["updated_at"].isoformat() if doc and doc.get("updated_at") else None
    finally:
        client.close()


def query_documents(uri: str, database: str, collection: str, filters: dict | None = None,
                    limit: int = 200000) -> list[dict]:
    """Read region records with optional Mongo query filters."""
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        cursor = client[database][collection].find(filters or {}, {"_id": 0}).limit(limit)
        return list(cursor)
    finally:
        client.close()


def summary_by_cluster(uri: str, database: str, collection: str,
                       adoption_field: str = "broadband_adoption_pct",
                       income_field: str | None = "median_income") -> list[dict]:
    """Aggregate cluster counts and mean measures in MongoDB."""
    averages = {"avg_download_mbps": {"$avg": "$avg_download_mbps"}}
    if adoption_field:
        averages["avg_adoption"] = {"$avg": f"${adoption_field}"}
    if income_field:
        averages["avg_income"] = {"$avg": f"${income_field}"}
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        pipeline = [
            {"$match": {"region_id": {"$exists": True}}},
            {"$group": {"_id": "$cluster_label", "region_count": {"$sum": 1}, **averages}},
            {"$sort": {"_id": 1}},
        ]
        return list(client[database][collection].aggregate(pipeline))
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="us")
    parser.add_argument("--input")
    parser.add_argument("--config")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    region = get_region(cfg, args.region)
    path = args.input or hdfs_path(cfg, region, "clusters")
    collection = region.get("mongo_collection", cfg["mongodb"]["collection"])
    spark = SparkSession.builder.appName(f"DigitalDivide-MongoWriter-{args.region}").getOrCreate()
    try:
        results = spark.read.parquet(path)
        count = write_spark_dataframe(results, cfg["mongodb"]["uri"], cfg["mongodb"]["database"],
                                      collection, args.batch_size,
                                      int(region.get("id_width", 0)))
        LOG.info("Upserted %d %s documents into MongoDB collection %s",
                 count, args.region, collection)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
