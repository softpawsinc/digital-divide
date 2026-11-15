"""Download quarterly Ookla Parquet tiles from its public S3 bucket."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pyarrow.parquet as pq

from src.config import load_config, resolve_path

LOG = logging.getLogger("download_ookla")


def list_parquet_keys(bucket: str, data_type: str, year: int, quarter: int,
                      region: str = "us-west-2") -> list[str]:
    """Return parquet object keys for one public Ookla quarter."""
    if data_type not in {"fixed", "mobile"}:
        raise ValueError("data_type must be 'fixed' or 'mobile'")
    if quarter not in range(1, 5):
        raise ValueError("quarter must be between 1 and 4")
    prefix = f"parquet/performance/type={data_type}/year={year}/quarter={quarter}/"
    client = boto3.client("s3", region_name=region, config=Config(signature_version=UNSIGNED))
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", [])
                    if obj["Key"].endswith(".parquet"))
    return sorted(keys)


def download_quarter(bucket: str, data_type: str, year: int, quarter: int,
                     destination: str | Path, limit: int | None = None,
                     region: str = "us-west-2") -> list[Path]:
    """Download files and inspect each footer for row count and required columns."""
    keys = list_parquet_keys(bucket, data_type, year, quarter, region)
    if limit is not None:
        keys = keys[:limit]
    if not keys:
        raise FileNotFoundError(
            f"No Parquet objects found for {data_type} {year} Q{quarter} in s3://{bucket}"
        )
    client = boto3.client("s3", region_name=region, config=Config(signature_version=UNSIGNED))
    destination = Path(destination) / f"type={data_type}" / f"year={year}" / f"quarter={quarter}"
    destination.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []
    for key in keys:
        target = destination / Path(key).name
        if not target.exists():
            LOG.info("Downloading s3://%s/%s", bucket, key)
            client.download_file(bucket, key, str(target))
        metadata = pq.ParquetFile(target).metadata
        schema = set(pq.ParquetFile(target).schema_arrow.names)
        required = {"avg_d_kbps", "avg_u_kbps", "avg_lat_ms", "tests", "devices", "tile"}
        missing = sorted(required - schema)
        if missing:
            target.unlink(missing_ok=True)
            raise ValueError(f"Unexpected Ookla schema in {key}; missing {missing}; found {sorted(schema)}")
        LOG.info("Validated %s (%d rows, %d row groups)", target, metadata.num_rows,
                 metadata.num_row_groups)
        downloaded.append(target)
    return downloaded


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--type", choices=["fixed", "mobile"])
    parser.add_argument("--year", type=int)
    parser.add_argument("--quarter", type=int)
    parser.add_argument("--limit", type=int, help="Download only the first N parquet files (smoke runs)")
    parser.add_argument("--config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    ookla = cfg["ookla"]
    paths = download_quarter(ookla["s3_bucket"], args.type or ookla["type"],
                             args.year or ookla["year"], args.quarter or ookla["quarter"],
                             resolve_path(ookla["local_dir"]), args.limit,
                             ookla.get("s3_region", "us-west-2"))
    LOG.info("Downloaded/validated %d parquet file(s)", len(paths))


if __name__ == "__main__":
    main()
