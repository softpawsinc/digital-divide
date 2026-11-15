"""Download ACS 5-year tract measures and TIGER/Line tract boundaries."""
from __future__ import annotations

import argparse
import logging
import os
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests

from src.config import load_config, resolve_path

LOG = logging.getLogger("download_census")
API_VARIABLES = [
    "NAME", "B28002_001E", "B28002_004E", "B19013_001E", "B15003_001E",
    "B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E",
]
NUMERIC_COLUMNS = [name for name in API_VARIABLES if name != "NAME"]


def parse_census_response(payload: list[list[str]]) -> pd.DataFrame:
    """Convert the Census API's header-plus-rows JSON to normalized tract data."""
    if not payload or len(payload) < 2:
        raise ValueError("Census API returned no tract records")
    frame = pd.DataFrame(payload[1:], columns=payload[0])
    required = set(API_VARIABLES + ["state", "county", "tract"])
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Census response is missing columns: {sorted(missing)}")
    for column in NUMERIC_COLUMNS:
        values = pd.to_numeric(frame[column], errors="coerce")
        frame[column] = values.mask(values < 0)
    frame["state"] = frame["state"].astype(str).str.zfill(2)
    frame["county"] = frame["county"].astype(str).str.zfill(3)
    frame["tract"] = frame["tract"].astype(str).str.zfill(6)
    frame["tract_id"] = frame["state"] + frame["county"] + frame["tract"]
    total_households = frame["B28002_001E"].replace({0: pd.NA})
    frame["broadband_adoption_pct"] = (100 * frame["B28002_004E"] / total_households).clip(0, 100)
    population_25 = frame["B15003_001E"].replace({0: pd.NA})
    college = frame[["B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E"]].sum(
        axis=1, min_count=1
    )
    frame["education_bachelors_pct"] = (100 * college / population_25).clip(0, 100)
    frame["median_income"] = frame["B19013_001E"].where(frame["B19013_001E"] >= 0)
    return frame[["tract_id", "state", "county", "tract", "NAME", "broadband_adoption_pct",
                  "median_income", "education_bachelors_pct", *NUMERIC_COLUMNS]]


def fetch_acs(year: int, api_key: str = "", retries: int = 4) -> pd.DataFrame:
    """Fetch tract-level ACS estimates, querying one state at a time."""
    if not api_key:
        raise RuntimeError(
            "A Census Data API key is required. Request a free key at "
            "https://api.census.gov/data/key_signup.html, then set CENSUS_API_KEY "
            "or census.api_key in config/config.yaml."
        )
    url = f"https://api.census.gov/data/{year}/acs/acs5"

    def request(params: list[tuple[str, str]], description: str) -> list[list[str]]:
        params.append(("key", api_key))
        error: Exception | None = None
        for attempt in range(retries):
            try:
                response = requests.get(url, params=params, timeout=(15, 180))
                response.raise_for_status()
                try:
                    return response.json()
                except ValueError as exc:
                    excerpt = " ".join(response.text.split())[:300]
                    content_type = response.headers.get("Content-Type", "unknown")
                    raise RuntimeError(
                        f"Census API returned non-JSON (HTTP {response.status_code}, "
                        f"Content-Type {content_type}): {excerpt}"
                    ) from exc
            except (requests.RequestException, ValueError) as exc:
                error = exc
                if attempt + 1 < retries:
                    wait = min(2 ** attempt, 20)
                    LOG.warning("Census %s request failed (%s); retrying in %ss",
                                description, exc, wait)
                    time.sleep(wait)
        raise RuntimeError(
            f"Census API request for {description} failed after {retries} attempts: {error}"
        )

    state_payload = request([("get", "NAME"), ("for", "state:*")], "state list")
    if not state_payload or len(state_payload) < 2 or "state" not in state_payload[0]:
        raise ValueError("Census API returned no state geography records")
    state_index = state_payload[0].index("state")
    state_codes = sorted({row[state_index].zfill(2) for row in state_payload[1:]})
    if not state_codes:
        raise ValueError("Census API returned an empty state geography list")

    frames = []
    for index, state in enumerate(state_codes, start=1):
        LOG.info("Fetching ACS tracts for state %s (%d/%d)", state, index, len(state_codes))
        params = [("get", ",".join(API_VARIABLES)), ("for", "tract:*"),
                  ("in", f"state:{state}"), ("in", "county:*")]
        payload = request(params, f"state {state}")
        frames.append(parse_census_response(payload))
    return pd.concat(frames, ignore_index=True)


def download_boundaries(url: str, destination: Path) -> Path:
    """Download state TIGER/Line archives and combine them into one tract shapefile."""
    import re
    import geopandas as gpd

    destination.mkdir(parents=True, exist_ok=True)
    match = re.search(r"TIGER(\d{4})", url) or re.search(r"tl_(\d{4})_", url)
    if not match:
        raise ValueError(f"Cannot determine TIGER vintage from URL: {url}")
    year = match.group(1)
    shp_path = destination / f"tl_{year}_us_tract.shp"
    complete_marker = destination / f"tl_{year}_us_tract.complete"
    companions = [shp_path, shp_path.with_suffix(".shx"), shp_path.with_suffix(".dbf"),
                  shp_path.with_suffix(".prj")]
    if complete_marker.exists() and all(path.exists() for path in companions):
        return shp_path

    # TIGER/Line publishes tract polygons as one ZIP per state/territory, not a
    # single nationwide ZIP. Accept either a directory URL or the old ZIP URL.
    listing_url = url.rsplit("/", 1)[0] + "/" if url.lower().endswith(".zip") \
        else url.rstrip("/") + "/"
    response = requests.get(listing_url, timeout=(20, 300))
    response.raise_for_status()
    archive_names = sorted(set(re.findall(
        rf"href=[\"'](tl_{year}_(\d{{2}})_tract\.zip)[\"']", response.text, re.IGNORECASE
    )))
    if not archive_names:
        raise FileNotFoundError(f"No state TIGER tract archives found at {listing_url}")

    complete_marker.unlink(missing_ok=True)
    part_root = destination / f"tract_parts_{year}"
    merged = False
    for archive_name, state in archive_names:
        state_dir = part_root / state
        state_dir.mkdir(parents=True, exist_ok=True)
        source_shp = state_dir / f"tl_{year}_{state}_tract.shp"
        if not source_shp.exists():
            zip_path = state_dir / archive_name
            if not zip_path.exists():
                zip_url = listing_url + archive_name
                LOG.info("Downloading TIGER tract boundaries for area %s", state)
                download = requests.get(zip_url, timeout=(20, 300), stream=True)
                download.raise_for_status()
                partial_path = zip_path.with_suffix(zip_path.suffix + ".part")
                with partial_path.open("wb") as output:
                    for chunk in download.iter_content(1024 * 1024):
                        if chunk:
                            output.write(chunk)
                partial_path.replace(zip_path)
            with zipfile.ZipFile(zip_path) as archive:
                prefix = f"tl_{year}_{state}_tract."
                for member in archive.namelist():
                    # Extract only files directly under this archive and guard traversal.
                    if Path(member).name != member or not member.startswith(prefix):
                        continue
                    archive.extract(member, state_dir)
        if not source_shp.exists():
            raise FileNotFoundError(f"TIGER archive did not contain {source_shp.name}")

        shapes = gpd.read_file(source_shp)
        shapes.to_file(shp_path, driver="ESRI Shapefile", index=False,
                       mode="a" if merged else "w")
        merged = True

    if not all(path.exists() for path in companions):
        raise FileNotFoundError(f"Could not create the combined TIGER tract shapefile at {shp_path}")
    complete_marker.write_text(f"{len(archive_names)} archives combined\n", encoding="utf-8")
    LOG.info("Combined %d TIGER tract archives into %s", len(archive_names), shp_path)
    return shp_path


def boundary_url_for_year(configured_url: str, year: int) -> str:
    """Use a matching TIGER/Line vintage when the configured URL is year-stamped."""
    import re
    return re.sub(r"TIGER\d{4}|tl_\d{4}_", lambda match: (
        f"TIGER{year}" if match.group(0).startswith("TIGER") else f"tl_{year}_"), configured_url)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int)
    parser.add_argument("--config")
    parser.add_argument("--skip-boundaries", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    census = cfg["census"]
    out_dir = resolve_path(census["local_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    year = args.year or census["year"]
    api_key = os.environ.get("CENSUS_API_KEY") or census.get("api_key", "")
    frame = fetch_acs(year, api_key)
    out_path = out_dir / f"acs5_tract_{year}.parquet"
    frame.to_parquet(out_path, index=False)
    LOG.info("Saved %d tract rows to %s", len(frame), out_path)
    if not args.skip_boundaries:
        shp_path = download_boundaries(boundary_url_for_year(census["tract_url"], year),
                                       out_dir / "boundaries")
        LOG.info("Boundary file: %s", shp_path)


if __name__ == "__main__":
    main()
