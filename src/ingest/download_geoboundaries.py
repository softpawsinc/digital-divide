"""Download geoBoundaries India ADM1/ADM2 and enrich districts with parent states."""
from __future__ import annotations

import argparse
import logging
import unicodedata
import urllib.request
from pathlib import Path

import geopandas as gpd

from src.config import load_config, resolve_path

LOG = logging.getLogger("download_geoboundaries")
RELEASE = "9469f09"
BASE = f"https://github.com/wmgeolab/geoBoundaries/raw/{RELEASE}/releaseData/gbOpen/IND"
URLS = {
    "adm1": f"{BASE}/ADM1/geoBoundaries-IND-ADM1_simplified.geojson",
    "adm2": f"{BASE}/ADM2/geoBoundaries-IND-ADM2_simplified.geojson",
}


def strip_diacritics(value: str) -> str:
    """Fold accents like Mahārāshtra -> Maharashtra for matching and display."""
    decomposed = unicodedata.normalize("NFKD", str(value))
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def download_file(url: str, target: Path, force: bool = False) -> Path:
    if target.exists() and not force:
        LOG.info("Reusing existing %s", target)
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, target)
    LOG.info("Downloaded %s", target)
    return target


def enrich_districts(adm2_path: Path, adm1_path: Path, output: Path) -> Path:
    """Attach the parent state name to each district and write the enriched GeoJSON."""
    districts = gpd.read_file(adm2_path).to_crs("EPSG:4326")
    states = gpd.read_file(adm1_path).to_crs("EPSG:4326")
    states = states.rename(columns={"shapeName": "state_name"})[["state_name", "geometry"]]
    states["state_name"] = states["state_name"].map(strip_diacritics)
    points = districts.copy()
    points["geometry"] = points.geometry.representative_point()
    joined = gpd.sjoin(points, states, how="left", predicate="within")
    joined = joined[~joined.index.duplicated(keep="first")]
    state_names = joined["state_name"].reindex(districts.index)
    missing = state_names.isna()
    if missing.any():
        LOG.warning("%d district representative points fell outside any state; using nearest",
                    int(missing.sum()))
        nearest = gpd.sjoin_nearest(points[missing], states[["state_name", "geometry"]])
        nearest = nearest[~nearest.index.duplicated(keep="first")]
        state_names.loc[nearest.index] = nearest["state_name"]
    districts["state_name"] = state_names.values
    districts["shapeName"] = districts["shapeName"].map(strip_diacritics)
    output.parent.mkdir(parents=True, exist_ok=True)
    districts.to_file(output, driver="GeoJSON", index=False)
    LOG.info("Wrote %d enriched districts to %s", len(districts), output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    region = cfg["regions"]["india"]
    boundary = resolve_path(region["boundary"])
    directory = boundary.parent
    adm1 = download_file(URLS["adm1"], directory / "geoBoundaries-IND-ADM1.geojson", args.force)
    adm2 = download_file(URLS["adm2"], directory / "geoBoundaries-IND-ADM2.geojson", args.force)
    print(enrich_districts(adm2, adm1, boundary))


if __name__ == "__main__":
    main()
