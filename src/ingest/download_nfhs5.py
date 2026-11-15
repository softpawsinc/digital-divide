"""Build district-level NFHS-5 internet and literacy features for India."""
from __future__ import annotations

import argparse
import difflib
import logging
import unicodedata
import urllib.request
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.config import load_config, resolve_path

LOG = logging.getLogger("download_nfhs5")
STATES_CSV = "https://raw.githubusercontent.com/pratapvardhan/NFHS-5/master/NFHS-5-States.csv"
INDICATORS = {
    "internet_women": 18,
    "internet_men": 19,
    "literate_women": 14,
    "literate_men": 15,
}


def normalize_name(value: str) -> str:
    """Lowercase, fold diacritics, and collapse punctuation for name matching."""
    value = str(value).replace("&", " and ")
    decomposed = unicodedata.normalize("NFKD", value)
    plain = "".join(char for char in decomposed if not unicodedata.combining(char))
    cleaned = "".join(char if char.isalnum() else " " for char in plain.casefold())
    return " ".join(cleaned.split())


def load_state_values(csv_path: Path) -> pd.DataFrame:
    """Pivot the long NFHS-5 state file into one row per state with two features."""
    frame = pd.read_csv(csv_path)
    frame["_number"] = pd.to_numeric(
        frame["indicator"].astype(str).str.extract(r"^(\d+)\.")[0], errors="coerce")
    records = {}
    for key, number in INDICATORS.items():
        matches = frame[frame["_number"] == number]
        if matches.empty:
            raise ValueError(f"NFHS-5 state file lacks indicator number {number}")
        records[key] = matches.drop_duplicates("state").set_index("state")["nfhs5_total"]
    wide = pd.DataFrame(records)
    wide["internet_use_pct"] = wide[["internet_women", "internet_men"]].mean(axis=1)
    wide["literacy_pct"] = wide[["literate_women", "literate_men"]].mean(axis=1)
    wide = wide.reset_index()[["state", "internet_use_pct", "literacy_pct"]]
    wide["_match"] = wide["state"].map(normalize_name)
    LOG.info("NFHS-5 state file covers %d states/UTs", len(wide))
    return wide


def match_state(state_name: str, lookup: dict[str, str]) -> str | None:
    """Match a geoBoundaries state label to the NFHS state naming."""
    normalized = normalize_name(state_name)
    if normalized in lookup:
        return lookup[normalized]
    contained = [key for key in lookup if normalized in key or key in normalized]
    if len(contained) == 1:
        return lookup[contained[0]]
    close = difflib.get_close_matches(normalized, lookup.keys(), n=1, cutoff=0.85)
    return lookup[close[0]] if close else None


def build_socio(boundary: Path, nfhs: pd.DataFrame) -> pd.DataFrame:
    """Broadcast state NFHS values to districts keyed by the enriched boundary file."""
    districts = gpd.read_file(boundary)
    required = {"shapeID", "shapeName", "state_name"}
    missing = required - set(districts.columns)
    if missing:
        raise ValueError(
            f"Enriched district file missing {sorted(missing)}; "
            "run src.ingest.download_geoboundaries first")
    lookup = dict(zip(nfhs["_match"], nfhs["state"]))
    states = districts["state_name"].drop_duplicates()
    mapping = {state: match_state(state, lookup) for state in states}
    unmatched = sorted(state for state, nfhs_state in mapping.items() if nfhs_state is None)
    if unmatched:
        LOG.warning("No NFHS-5 match for %d states: %s", len(unmatched), unmatched)
    districts["nfhs_state"] = districts["state_name"].map(mapping)
    merged = districts.merge(nfhs, left_on="nfhs_state", right_on="state", how="left")
    frame = pd.DataFrame({
        "region_id": merged["shapeID"].astype(str),
        "region_name": merged["shapeName"].astype(str),
        "parent_1": merged["state_name"].astype(str),
        "nfhs_state": merged["nfhs_state"],
        "internet_use_pct": pd.to_numeric(merged["internet_use_pct"], errors="coerce"),
        "literacy_pct": pd.to_numeric(merged["literacy_pct"], errors="coerce"),
    })
    covered = frame["internet_use_pct"].notna().sum()
    LOG.info("NFHS-5 values available for %d of %d districts", covered, len(frame))
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    region = cfg["regions"]["india"]
    raw_csv = resolve_path(region["socio"]).parent / "nfhs5_states.csv"
    if raw_csv.exists() and not args.force:
        LOG.info("Reusing existing %s", raw_csv)
    else:
        raw_csv.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(STATES_CSV, raw_csv)
        LOG.info("Downloaded %s", raw_csv)
    nfhs = load_state_values(raw_csv)
    frame = build_socio(resolve_path(region["boundary"]), nfhs)
    output = Path(args.output) if args.output else resolve_path(region["socio"])
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(output, index=False)
    LOG.info("Wrote district socioeconomic features to %s", output)


if __name__ == "__main__":
    main()
