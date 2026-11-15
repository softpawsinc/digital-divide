# Laptop setup guide

This guide runs the project locally with Spark inside Docker, HDFS and MongoDB containers, and the dashboard in your Python environment.

## 1. Install prerequisites

- Docker Engine/Desktop with `docker compose` (Compose v2).
- Python 3.9 or newer; Python 3.10/3.11 are the most tested dependency targets.
- Java 11 or 17 if you plan to run the local PySpark tests or notebooks outside the container.
- Git (optional, if you clone the project).
- At least 8 GB free RAM is recommended for the HDFS/Spark containers. The full Ookla quarter and Census boundaries need additional disk space.

Check versions:

```bash
docker --version
docker compose version
python --version
java -version
```

## 2. Create a Python environment

From the project directory:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

The host environment is used for downloading source data, the dashboard, and tests. Spark and Hadoop client dependencies are installed in the `spark` service image.

## 3. Start Docker services

```bash
docker compose -f docker/docker-compose.yml up -d --build --wait
docker compose -f docker/docker-compose.yml ps
```

Open `http://localhost:9870` to view HDFS. MongoDB is available on `localhost:27017` with the development login `admin` / `secret`. To stop containers while retaining data, run `docker compose -f docker/docker-compose.yml down`; add `-v` only if you intend to delete persisted HDFS and MongoDB data.

## 4. Select and download data

The checked-in default uses Ookla fixed broadband for 2023 Q2 and ACS 2022 five-year estimates. Start the full run with:

```bash
scripts/run_pipeline.sh
```

Or select the source quarter/type:

```bash
OOKLA_TYPE=fixed OOKLA_YEAR=2023 OOKLA_QUARTER=2 scripts/run_pipeline.sh
```

The Ookla S3 bucket is public and accessed anonymously. `OOKLA_FILE_LIMIT=1` restricts ingestion to the first listed Parquet file for smoke runs. Census API calls require a free API key. Request one at https://api.census.gov/data/key_signup.html, then set `CENSUS_API_KEY` in the terminal or configure `census.api_key` in `config/config.yaml`. Census JSON becomes `data/census/acs5_tract_<year>.parquet`. TIGER/Line tract boundaries are downloaded from the state-level ZIP archives and combined into `data/census/boundaries/tl_<year>_us_tract.shp`.

The pipeline reuses those Census inputs when the Parquet file and combined shapefile are already present. Set `FORCE_CENSUS_DOWNLOAD=1` to download them again.

For separate ingestion:

```bash
python -m src.ingest.download_ookla --type fixed --year 2023 --quarter 2 --limit 1
python -m src.ingest.download_census --year 2022
```

## 5. Run the stages manually

The top-level script performs these commands in order. It uploads local input files through the Hadoop CLI in the Spark container, then submits the PySpark jobs against HDFS.

```bash
scripts/init_hdfs.sh
docker compose -f docker/docker-compose.yml exec -T spark python3 -m src.ingest.upload_to_hdfs
docker compose -f docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' --driver-memory 2g /workspace/src/process/clean_tiles.py
docker compose -f docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' --driver-memory 2g /workspace/src/process/geo_join.py --boundaries /workspace/data/census/boundaries
docker compose -f docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' --driver-memory 2g /workspace/src/process/feature_engineering.py
docker compose -f docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' --driver-memory 2g /workspace/src/model/kmeans_clustering.py
docker compose -f docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' --driver-memory 2g /workspace/src/serve/mongo_writer.py
docker compose -f docker/docker-compose.yml exec -T spark python3 -m src.visualize.export_geojson
```

Each HDFS stage defaults to `/user/digitaldivide`. Pass `--input` and `--output` flags to a job to override its paths. The YAML file is `config/config.yaml`; overrides use `DIGITAL_DIVIDE_` with nested keys separated by `__`, such as `DIGITAL_DIVIDE_MODEL__K_MAX=6`.

## 6. Launch the dashboard

Run from the project root with the Python environment active:

```bash
streamlit run src/visualize/dashboard.py
```

The default dashboard URI is `mongodb://admin:secret@localhost:27017/?authSource=admin`, matching the local Docker port. If you changed the credentials, set `MONGODB_URI` before launching. The map export is regenerated at `data/processed/clusters.geojson` by the pipeline.

## 7. Run tests

```bash
pytest
```

The Spark tests use local mode and do not require Docker. No live data download is performed by tests.

## Troubleshooting

### A container is unhealthy or `--wait` times out

Inspect `docker compose -f docker/docker-compose.yml ps` and `docker compose -f docker/docker-compose.yml logs namenode datanode spark mongo`. First HDFS startup may take a few minutes. Confirm Docker has enough memory, then retry `up -d --wait`.

### Spark cannot find a DataNode

The Spark job must run inside the `spark` Compose service so HDFS returns the internal `datanode` service address. Check that both HDFS services are healthy and that `docker compose ... exec spark hdfs dfs -ls /` succeeds.

### MongoDB authentication fails

The default root username/password are configured in `docker/docker-compose.yml`. If the MongoDB volume was initialized with different credentials, change the URI in `config/config.yaml` and the Spark environment in Compose to match. MongoDB only applies the initial root variables when creating a fresh data directory.

### Census API returns an error or no records

Check the requested ACS year, API key, and network connection. Census API calls require `CENSUS_API_KEY` or `census.api_key` in the config. The request uses tract, state, and county wildcards and retries transient HTTP errors with backoff.

### Ookla download fails or fills the disk

Confirm the selected type/year/quarter exists in the public bucket. Use `--limit 1` / `OOKLA_FILE_LIMIT=1` before downloading a large quarter. Local downloads are retained and reused after Parquet footer/schema validation.

### No tracts appear in the map

The map includes only TIGER tracts with at least one cleaned Ookla tile and a cluster output. Check `processed/tiles_with_tract` and `processed/clustered_tracts` in HDFS; mobile or fixed tiles outside the U.S. are intentionally unmatched to U.S. tract geography.
