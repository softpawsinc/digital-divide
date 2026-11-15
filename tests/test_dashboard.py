from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from src.serve import mongo_writer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "2024-01-01T00:00:00+00:00"
TEST_DATABASE = "digital_divide_tests"

TIER_DOCUMENTS = {
    "clusters": {"level_label": "Tracts", "socio": {
        "broadband_adoption_pct": 88.0, "median_income": 55000.0}},
    "clusters_india": {"level_label": "Districts", "socio": {
        "internet_use_pct": 45.0, "literacy_pct": 75.0}},
    "clusters_world": {"level_label": "Countries", "socio": {
        "internet_users_pct": 81.0, "gdp_per_capita": 32000.0}},
}


def _documents(collection: str) -> list[dict]:
    """Two synthetic regions per tier with fields the dashboard can adapt to."""
    spec = TIER_DOCUMENTS[collection]
    rows = []
    for index, download in enumerate((20.0, 120.0)):
        label = "underserved" if index == 0 else "well-served"
        row = {
            "region_id": f"{collection[:4]}-{index}",
            "region_name": f"Region {index}",
            "parent_1": f"P{index}",
            "cluster_id": index,
            "cluster_label": label,
            "avg_download_mbps": download,
            "avg_upload_mbps": download / 4,
            "avg_latency_ms": 30.0,
            "updated_at": RUN_ID,
        }
        row.update(spec["socio"])
        rows.append(row)
    return rows


def _patch_mongo(monkeypatch, seen: list[str]) -> None:
    # A distinct database per test keeps st.cache_data keys from colliding
    # with results cached by other tests in the same process.
    monkeypatch.setenv("DIGITAL_DIVIDE_MONGODB__DATABASE", TEST_DATABASE)
    monkeypatch.setattr(mongo_writer, "latest_run_id",
                        lambda uri, database, collection: RUN_ID)
    monkeypatch.setattr(
        mongo_writer, "query_documents",
        lambda uri, database, collection, **kwargs: (seen.append(collection),
                                                     _documents(collection))[1])


def test_dashboard_switches_tiers_and_reads_matching_collection(monkeypatch):
    collections: list[str] = []
    monkeypatch.setattr(
        mongo_writer, "latest_run_id",
        lambda uri, database, collection: collections.append(collection) or None,
    )
    monkeypatch.setattr(mongo_writer, "query_documents", lambda *args, **kwargs: [])

    app = AppTest.from_file(
        str(PROJECT_ROOT / "src/visualize/dashboard.py"), default_timeout=30
    ).run()
    tiers = [
        ("US Tracts", "clusters"),
        ("India Districts", "clusters_india"),
        ("World Countries", "clusters_world"),
    ]

    assert app.selectbox(key="dashboard_region").options == [label for label, _ in tiers]
    assert not app.exception
    for label, _ in tiers:
        app.selectbox(key="dashboard_region").set_value(label).run()
        assert not app.exception

    assert collections == [collection for _, collection in tiers]


def test_dashboard_rebuilds_filters_for_each_tier(monkeypatch):
    seen: list[str] = []
    _patch_mongo(monkeypatch, seen)
    app = AppTest.from_file(
        str(PROJECT_ROOT / "src/visualize/dashboard.py"), default_timeout=60
    ).run()
    assert not app.exception

    assert [metric.label for metric in app.metric] == ["Tracts", "Segments",
                                                       "Median download",
                                                       "Median broadband_adoption_pct"]
    app.selectbox(key="dashboard_region").set_value("India Districts").run()
    assert not app.exception
    assert [metric.label for metric in app.metric] == ["Districts", "Segments",
                                                       "Median download",
                                                       "Median internet_use_pct"]
    assert app.multiselect(key="labels_india").value == ["underserved", "well-served"]

    app.selectbox(key="dashboard_region").set_value("World Countries").run()
    assert not app.exception
    assert [metric.label for metric in app.metric] == ["Countries", "Segments",
                                                       "Median download",
                                                       "Median internet_users_pct"]
    assert app.multiselect(key="labels_world").value == ["underserved", "well-served"]
    assert seen == ["clusters", "clusters_india", "clusters_world"]
