"""Offline unit coverage for external source parsing and S3 listing."""
from src.ingest.download_census import parse_census_response
from src.ingest.download_ookla import list_parquet_keys


def test_parse_census_response_derives_rates():
    headers = ["NAME", "B28002_001E", "B28002_004E", "B19013_001E", "B15003_001E",
               "B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E", "state", "county", "tract"]
    values = ["Example County, State", "100", "60", "55000", "200", "30", "10", "2", "1",
              "06", "001", "000100"]
    result = parse_census_response([headers, values]).iloc[0]
    assert result.tract_id == "06001000100"
    assert result.broadband_adoption_pct == 60
    assert result.education_bachelors_pct == 21.5
    assert result.median_income == 55000


def test_list_parquet_keys_uses_public_bucket_prefix(monkeypatch):
    class Paginator:
        def paginate(self, **kwargs):
            assert kwargs == {"Bucket": "ookla-open-data",
                              "Prefix": "parquet/performance/type=fixed/year=2023/quarter=2/"}
            return [{"Contents": [{"Key": "z.parquet"}, {"Key": "folder/skip.txt"},
                                   {"Key": "a.parquet"}]}]

    class Client:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return Paginator()

    monkeypatch.setattr("src.ingest.download_ookla.boto3.client", lambda *a, **k: Client())
    assert list_parquet_keys("ookla-open-data", "fixed", 2023, 2) == ["a.parquet", "z.parquet"]
