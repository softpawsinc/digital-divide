"""Download Natural Earth 1:110m country boundaries for the world tier."""
from __future__ import annotations

import argparse
import logging
import urllib.request
import zipfile
from pathlib import Path

from src.config import load_config, resolve_path

LOG = logging.getLogger("download_world")
URL = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"


def download(destination: Path, force: bool = False) -> Path:
    """Fetch and unpack the Natural Earth admin-0 shapefile."""
    shapefile = destination / "ne_110m_admin_0_countries.shp"
    if shapefile.exists() and not force:
        LOG.info("Reusing existing %s", shapefile)
        return shapefile
    destination.mkdir(parents=True, exist_ok=True)
    archive = destination / "ne_110m_admin_0_countries.zip"
    urllib.request.urlretrieve(URL, archive)
    with zipfile.ZipFile(archive) as bundle:
        bundle.extractall(destination)
    if not shapefile.exists():
        raise FileNotFoundError(f"Archive did not contain {shapefile.name}")
    LOG.info("Downloaded country boundaries to %s", shapefile)
    return shapefile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    destination = resolve_path(cfg["regions"]["world"]["boundary"]).parent
    print(download(destination, args.force))


if __name__ == "__main__":
    main()
