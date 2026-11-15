"""Download World Bank indicators and reshape them into country-level features."""
from __future__ import annotations

import argparse
import json
import logging
import urllib.request
from pathlib import Path

import pandas as pd

from src.config import load_config, resolve_path

LOG = logging.getLogger("download_worldbank")
API = "https://api.worldbank.org/v2/country/all/indicator/{code}"
INDICATORS = {
    "IT.NET.USER.ZS": "internet_users_pct",
    "NY.GDP.PCAP.PP.CD": "gdp_per_capita",
    "SP.POP.TOTL": "population",
}
# World Bank ISO3 codes that differ from Natural Earth ADM0_A3 codes.
NE_CODE_ALIASES = {"SLB": "SOL", "PSE": "PSX", "XKX": "KOS"}


def fetch_indicator(code: str, date_range: str = "2010:2024") -> dict[str, tuple[int, float]]:
    """Return {country_iso3: (year, value)} using the latest non-null observation."""
    url = f"{API.format(code=code)}?format=json&per_page=20000&date={date_range}"
    with urllib.request.urlopen(url, timeout=60) as response:
        payload = json.load(response)
    if len(payload) < 2 or not payload[1]:
        raise ValueError(f"World Bank API returned no data for {code}")
    latest: dict[str, tuple[int, float]] = {}
    for entry in payload[1]:
        iso3 = (entry.get("countryiso3code") or "").strip()
        value = entry.get("value")
        if len(iso3) != 3 or value is None:
            continue
        year = int(entry["date"])
        known = latest.get(iso3)
        if known is None or year > known[0]:
            latest[iso3] = (year, float(value))
    LOG.info("Indicator %s: %d countries", code, len(latest))
    return latest


def build_indicators() -> pd.DataFrame:
    """Merge the three indicators into one frame keyed by Natural Earth codes."""
    series: dict[str, dict[str, float]] = {}
    names: dict[str, str] = {}
    for code, column in INDICATORS.items():
        latest = fetch_indicator(code)
        series[column] = {iso3: value for iso3, (_, value) in latest.items()}
    url = "https://api.worldbank.org/v2/country?format=json&per_page=400"
    with urllib.request.urlopen(url, timeout=60) as response:
        countries = json.load(response)[1]
    for entry in countries:
        iso3 = entry.get("id", "")
        if len(iso3) == 3 and entry.get("region", {}).get("id") != "NA":
            names[iso3] = entry.get("name", iso3)
    rows = []
    for iso3 in sorted(series["internet_users_pct"]):
        rows.append({
            "region_id": NE_CODE_ALIASES.get(iso3, iso3),
            "wb_code": iso3,
            "country_name": names.get(iso3, iso3),
            **{column: values.get(iso3) for column, values in series.items()},
        })
    frame = pd.DataFrame(rows)
    LOG.info("World Bank features for %d countries", len(frame))
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output")
    parser.add_argument("--config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    output = Path(args.output) if args.output else resolve_path(cfg["regions"]["world"]["socio"])
    output.parent.mkdir(parents=True, exist_ok=True)
    build_indicators().to_parquet(output, index=False)
    LOG.info("Wrote country indicators to %s", output)


if __name__ == "__main__":
    main()
