"""Copy local input files into the configured HDFS data lake using hdfs CLI."""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from src.config import load_config, resolve_path


def run_hdfs(*args: str) -> str:
    result = subprocess.run(["hdfs", "dfs", *args], check=True, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.strip()


def upload_tree(local_dir: Path, hdfs_dir: str) -> int:
    if not local_dir.exists():
        raise FileNotFoundError(f"Local input directory does not exist: {local_dir}")
    run_hdfs("-mkdir", "-p", hdfs_dir)
    files = [path for path in local_dir.rglob("*") if path.is_file()]
    if not files:
        raise FileNotFoundError(f"No files found under {local_dir}")
    for path in files:
        relative_parent = path.parent.relative_to(local_dir)
        target_dir = hdfs_dir if str(relative_parent) == "." else f"{hdfs_dir}/{relative_parent.as_posix()}"
        run_hdfs("-mkdir", "-p", target_dir)
        run_hdfs("-put", "-f", str(path), f"{target_dir}/{path.name}")
    listing = run_hdfs("-ls", "-R", hdfs_dir)
    print(listing)
    return len(files)


def upload_file(local_file: Path, hdfs_dir: str) -> int:
    """Upload one input file without copying unrelated local artifacts."""
    if not local_file.is_file():
        raise FileNotFoundError(f"Local input file does not exist: {local_file}")
    run_hdfs("-mkdir", "-p", hdfs_dir)
    run_hdfs("-put", "-f", str(local_file), f"{hdfs_dir}/{local_file.name}")
    print(run_hdfs("-ls", f"{hdfs_dir}/{local_file.name}"))
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    args = parser.parse_args()
    cfg = load_config(args.config)
    base = cfg["hdfs"]["base_path"].rstrip("/")
    run_hdfs("-mkdir", "-p", f"{base}/raw/ookla", f"{base}/raw/census", f"{base}/processed")
    count_ookla = upload_tree(resolve_path(cfg["ookla"]["local_dir"]), f"{base}/raw/ookla")
    census_file = resolve_path(cfg["census"]["local_dir"]) / \
        f"acs5_tract_{cfg['census']['year']}.parquet"
    if census_file.is_file():
        count_census = upload_file(census_file, f"{base}/raw/census")
    else:
        count_census = 0
        print(f"Skipping Census upload; {census_file} is not present.")
    print(f"Uploaded {count_ookla} Ookla file(s) and {count_census} Census file(s).")


if __name__ == "__main__":
    main()
