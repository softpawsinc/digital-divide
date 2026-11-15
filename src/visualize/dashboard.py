"""Streamlit dashboard for regional cluster outputs served by MongoDB."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Streamlit puts only this script's directory on sys.path; add the project
# root so src.* imports resolve with `streamlit run src/visualize/dashboard.py`.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import load_config, resolve_path
from src.serve.mongo_writer import latest_run_id, query_documents

LEVEL_LABELS = {"tract": "Tracts", "district": "Districts", "country": "Countries"}
DASHBOARD_REGION_LABELS = {
    "us": "US Tracts",
    "india": "India Districts",
    "world": "World Countries",
}


@st.cache_data(ttl=120)
def load_data(uri: str, database: str, collection: str) -> tuple[str | None, pd.DataFrame]:
    run_id = latest_run_id(uri, database, collection)
    if not run_id:
        return None, pd.DataFrame()
    documents = query_documents(uri, database, collection, limit=250000)
    frame = pd.DataFrame(documents)
    if "updated_at" in frame:
        frame["updated_at"] = pd.to_datetime(frame["updated_at"], utc=True, errors="coerce")
        newest = frame["updated_at"].max()
        frame = frame[frame["updated_at"] == newest]
    return run_id, frame


@st.cache_data(show_spinner=False)
def load_geojson(path: str) -> dict:
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def active_region(cfg: dict) -> tuple[str, dict]:
    """Pick the configured region whose Mongo collection is selected."""
    collection = cfg["mongodb"]["collection"]
    for name, region in (cfg.get("regions") or {}).items():
        if region.get("mongo_collection", collection) == collection:
            return name, region
    return "us", (cfg.get("regions") or {}).get("us", {})


def dashboard_region_options(cfg: dict) -> tuple[list[str], dict[str, str]]:
    """Build sidebar labels and map them to configured region keys."""
    regions = cfg.get("regions") or {}
    label_to_name: dict[str, str] = {}
    for name, region in regions.items():
        label = DASHBOARD_REGION_LABELS.get(name)
        if label is None:
            level = LEVEL_LABELS.get(region.get("level", "region"), "Regions")
            label = f"{region.get('label', name)} — {level}"
        label_to_name[label] = name
    return list(label_to_name), label_to_name


def canonical_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize legacy tract-era fields to the shared region schema."""
    if "region_id" not in frame and "tract_id" in frame:
        frame["region_id"] = frame["tract_id"]
    if "parent_1" not in frame and "state" in frame:
        frame["parent_1"] = frame["state"]
    if "parent_2" not in frame and "county" in frame:
        frame["parent_2"] = frame["county"]
    return frame


def first_present(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in frame.columns:
            return name
    return None


def geojson_bounds(features: list[dict]) -> tuple[float, float, float, float] | None:
    """Return (min_lat, max_lat, min_lon, max_lon) across the given features."""
    lats: list[float] = []
    lons: list[float] = []

    def walk(coords) -> None:
        if not coords:
            return
        if isinstance(coords[0], (int, float)):
            lons.append(float(coords[0]))
            lats.append(float(coords[1]))
        else:
            for item in coords:
                walk(item)

    for feature in features:
        geometry = feature.get("geometry") or {}
        walk(geometry.get("coordinates"))
    if not lats:
        return None
    return min(lats), max(lats), min(lons), max(lons)


def zoom_for_span(span: float) -> float:
    """Approximate a MapLibre zoom level from a degree span."""
    for threshold, zoom in ((40, 2.5), (20, 3), (10, 4), (5, 5), (2, 6)):
        if span > threshold:
            return zoom
    return 7


def main() -> None:
    st.set_page_config(page_title="Digital Divide Explorer", page_icon="🌐", layout="wide")
    st.title("Digital Divide Explorer")
    cfg = load_config()
    configured_region_name, _ = active_region(cfg)
    region_labels, label_to_name = dashboard_region_options(cfg)
    configured_label = next(
        (label for label, name in label_to_name.items() if name == configured_region_name),
        region_labels[0] if region_labels else "",
    )
    with st.sidebar:
        st.header("Dashboard")
        selected_region_label = st.selectbox(
            "Data tier", region_labels, index=region_labels.index(configured_label),
            key="dashboard_region",
        )
    region_name = label_to_name[selected_region_label]
    region = cfg["regions"][region_name]
    level_label = LEVEL_LABELS.get(region.get("level", "region"), "Regions")
    st.caption(f"{region.get('label', region_name)} — Ookla performance joined with "
               "census, survey, and World Bank estimates")
    mongo = cfg["mongodb"]
    collection = region.get("mongo_collection", mongo["collection"])
    try:
        run_id, all_data = load_data(mongo["uri"], mongo["database"], collection)
    except Exception as exc:  # Show actionable connection hints in the UI.
        st.error(f"Could not connect to MongoDB: {exc}")
        st.info("Start Docker and run scripts/run_pipeline.sh for this tier first.")
        return
    if all_data.empty:
        st.info("No results yet. Download data and run the pipeline to populate MongoDB.")
        return
    all_data = canonical_columns(all_data)
    st.caption(f"Latest pipeline run: {run_id}")

    adoption_field = first_present(all_data, ["broadband_adoption_pct", "internet_use_pct",
                                              "internet_users_pct"])
    income_field = first_present(all_data, ["median_income", "gdp_per_capita"])
    measure_fields = [name for name in ("avg_download_mbps", "avg_upload_mbps", "avg_latency_ms")
                      if name in all_data.columns]
    parental = sorted(all_data["parent_1"].dropna().astype(str).unique().tolist()) \
        if "parent_1" in all_data.columns else []
    labels = sorted(all_data["cluster_label"].dropna().astype(str).unique().tolist())
    # Filter widgets carry the region name in their key so switching tiers
    # rebuilds them with defaults for the newly selected data.
    with st.sidebar:
        st.header("Filters")
        selected_parents = st.multiselect("Parent region", parental, default=[],
                                          key=f"parents_{region_name}") \
            if parental else []
        selected_labels = st.multiselect("Cluster segment", labels, default=labels,
                                         key=f"labels_{region_name}")
        income_range = None
        if income_field:
            income_values = pd.to_numeric(all_data[income_field], errors="coerce")
            income_max = float(income_values.max()) if income_values.notna().any() else 0.0
            income_range = st.slider(income_field, 0.0, income_max, (0.0, income_max),
                                     step=max(1.0, income_max / 100),
                                     key=f"income_{region_name}")
        map_options = ["cluster_label"] + measure_fields + \
            ([adoption_field] if adoption_field else [])
        measure = st.selectbox("Map measure", map_options,
                               key=f"measure_{region_name}")
    frame = all_data.copy()
    if selected_parents and "parent_1" in frame.columns:
        frame = frame[frame["parent_1"].astype(str).isin(selected_parents)]
    frame = frame[frame["cluster_label"].astype(str).isin(selected_labels)]
    if income_field and income_range:
        income = pd.to_numeric(frame[income_field], errors="coerce")
        frame = frame[income.between(*income_range)]
    if frame.empty:
        st.info("No regions match the current filters.")
        return

    counts = frame["cluster_label"].value_counts()
    cols = st.columns(4)
    cols[0].metric(level_label, f"{len(frame):,}")
    cols[1].metric("Segments", str(frame["cluster_id"].nunique()))
    download = pd.to_numeric(frame["avg_download_mbps"], errors="coerce")
    cols[2].metric("Median download", f"{download.median():.1f} Mbps")
    if adoption_field:
        adoption = pd.to_numeric(frame[adoption_field], errors="coerce")
        cols[3].metric(f"Median {adoption_field}", f"{adoption.median():.1f}%")

    left, right = st.columns([1.2, 1])
    with left:
        st.subheader("Cluster distribution")
        chart = px.bar(counts.rename_axis("cluster_label").reset_index(name="regions"),
                       x="cluster_label", y="regions", color="cluster_label")
        st.plotly_chart(chart, width="stretch")
    with right:
        st.subheader("Feature distributions")
        dist_options = measure_fields + \
            [name for name in (adoption_field, income_field, "education_bachelors_pct",
                               "literacy_pct") if name and name in frame.columns]
        dist_feature = st.selectbox("Feature", dist_options,
                                    key=f"dist_feature_{region_name}")
        dist = px.box(frame, x="cluster_label", y=dist_feature, color="cluster_label",
                      points=False)
        st.plotly_chart(dist, width="stretch")

    geo_path = resolve_path(region.get("geojson", "data/processed/clusters.geojson"))
    st.subheader(f"{level_label} map")
    if geo_path.exists():
        try:
            geojson = load_geojson(str(geo_path))
            selected_ids = set(frame["region_id"].astype(str))
            filtered_geojson = {"type": "FeatureCollection", "features": [
                feature for feature in geojson["features"]
                if str(feature.get("properties", {}).get("region_id", "")) in selected_ids
            ]}
            if filtered_geojson["features"]:
                map_frame = frame.copy()
                map_frame["region_id"] = map_frame["region_id"].astype(str)
                hover_columns = [name for name in ("cluster_label", "avg_download_mbps",
                                                   adoption_field, income_field)
                                 if name and name in map_frame.columns]
                bounds = geojson_bounds(filtered_geojson["features"])
                if bounds:
                    min_lat, max_lat, min_lon, max_lon = bounds
                    center = {"lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2}
                    zoom = zoom_for_span(max(max_lat - min_lat, max_lon - min_lon))
                else:
                    center, zoom = {"lat": 39.5, "lon": -98.35}, 3
                fig = px.choropleth_map(
                    map_frame, geojson=filtered_geojson, locations="region_id",
                    featureidkey="properties.region_id", color=measure,
                    hover_name="region_id" if "region_name" not in map_frame.columns
                    else "region_name",
                    hover_data=hover_columns, map_style="carto-positron", center=center,
                    zoom=zoom, opacity=0.65, height=650)
                st.plotly_chart(fig, width="stretch")
            else:
                st.warning("No matching shapes found for these filters. "
                           "Re-export the GeoJSON if labels changed.")
        except Exception as exc:
            st.warning(f"Could not draw the GeoJSON map: {exc}")
    else:
        st.info("Map file not found. Run `src/visualize/export_geojson.py` after the cluster load.")

    st.subheader(f"Highest and lowest access {level_label.lower()}")
    rank_options = [name for name in ("avg_download_mbps", adoption_field, income_field)
                    if name and name in frame.columns]
    rank_by = st.selectbox("Rank by", rank_options, key=f"rank_by_{region_name}")
    table_cols = [name for name in
                  ["region_id", "region_name", "parent_1", "parent_2", "cluster_label",
                   rank_by, "avg_latency_ms", adoption_field, income_field]
                  if name and name in frame.columns]
    low, high = st.columns(2)
    low.caption("Lowest")
    low.dataframe(frame.nsmallest(10, rank_by)[table_cols], width="stretch", hide_index=True)
    high.caption("Highest")
    high.dataframe(frame.nlargest(10, rank_by)[table_cols], width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
