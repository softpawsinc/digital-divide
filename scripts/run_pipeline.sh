#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
# Prefer a project virtual environment for host-side ingestion steps.
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  for candidate in "$PROJECT_ROOT/.venv311" "$PROJECT_ROOT/.venv"; do
    if [[ -x "$candidate/bin/python" ]]; then
      export PATH="$candidate/bin:$PATH"
      break
    fi
  done
fi
COMPOSE=(docker compose -f docker/docker-compose.yml)
REGION="${REGION:-us}"
LOG_DIR="$PROJECT_ROOT/data/processed/logs"
mkdir -p "$LOG_DIR"
# The Spark container runs as a non-root UID; local artifact writes
# (model metrics, evaluation plot, GeoJSON) need write access to this dir.
chmod 777 "$PROJECT_ROOT/data/processed"
exec > >(tee -a "$LOG_DIR/pipeline-${REGION}-$(date +%Y%m%d-%H%M%S).log") 2>&1

config_value() {
  python - "$1" <<'PY'
import sys
from src.config import load_config
value = load_config()
for key in sys.argv[1].split("."):
    value = value[key]
print(value)
PY
}

OOKLA_TYPE="${OOKLA_TYPE:-$(config_value ookla.type)}"
OOKLA_YEAR="${OOKLA_YEAR:-$(config_value ookla.year)}"
OOKLA_QUARTER="${OOKLA_QUARTER:-$(config_value ookla.quarter)}"
CENSUS_YEAR="${CENSUS_YEAR:-$(config_value census.year)}"
HDFS_BASE="${HDFS_BASE_PATH:-$(config_value hdfs.base_path)}"
SPARK_MASTER_ARG="$(config_value spark.master)"
SPARK_DRIVER_MEMORY="$(config_value spark.driver_memory)"

COMPOSE_ENV_ARGS=()
while IFS= read -r env_entry; do
  case "$env_entry" in
    DIGITAL_DIVIDE_*=*|HDFS_NAMENODE=*|HDFS_BASE_PATH=*|MONGODB_URI=*|MONGODB_DATABASE=*|MONGODB_COLLECTION=*)
      COMPOSE_ENV_ARGS+=(-e "$env_entry") ;;
  esac
done < <(env)

compose_exec() {
  "${COMPOSE[@]}" exec -T "${COMPOSE_ENV_ARGS[@]}" "$@"
}

echo "Starting HDFS, MongoDB, and the Spark runtime..."
if [[ "${SKIP_DOCKER_UP:-0}" == "1" ]]; then
  echo "Using the Docker services started by the all-in-one runner."
else
  "${COMPOSE[@]}" up -d --build --wait
fi

echo "Downloading public Ookla data..."
OOKLA_ARGS=()
[[ -n "${OOKLA_FILE_LIMIT:-}" ]] && OOKLA_ARGS+=(--limit "$OOKLA_FILE_LIMIT")
python -m src.ingest.download_ookla --type "$OOKLA_TYPE" --year "$OOKLA_YEAR" \
  --quarter "$OOKLA_QUARTER" "${OOKLA_ARGS[@]}"

if [[ "$REGION" == "us" ]]; then
  CENSUS_DIR="$(config_value census.local_dir)"
  CENSUS_PARQUET="$CENSUS_DIR/acs5_tract_${CENSUS_YEAR}.parquet"
  CENSUS_BOUNDARY="$CENSUS_DIR/boundaries/tl_${CENSUS_YEAR}_us_tract.shp"
  if [[ "${FORCE_CENSUS_DOWNLOAD:-0}" != "1" && -s "$CENSUS_PARQUET" \
        && -s "$CENSUS_BOUNDARY" && -s "${CENSUS_BOUNDARY%.shp}.shx" \
        && -s "${CENSUS_BOUNDARY%.shp}.dbf" && -s "${CENSUS_BOUNDARY%.shp}.prj" ]]; then
    echo "Reusing existing ACS and TIGER/Line inputs."
  else
    echo "Downloading ACS estimates and TIGER/Line tract boundaries..."
    python -m src.ingest.download_census --year "$CENSUS_YEAR"
  fi
elif [[ "$REGION" == "india" ]]; then
  echo "Downloading geoBoundaries districts and NFHS-5 state indicators..."
  python -m src.ingest.download_geoboundaries
  python -m src.ingest.download_nfhs5
elif [[ "$REGION" == "world" ]]; then
  echo "Downloading Natural Earth boundaries and World Bank indicators..."
  python -m src.ingest.download_world
  python -m src.ingest.download_worldbank
else
  echo "Unknown REGION '$REGION'; expected us, india, or world." >&2
  exit 1
fi

echo "Initializing HDFS paths and uploading inputs..."
HDFS_BASE_PATH="$HDFS_BASE" scripts/init_hdfs.sh
compose_exec spark python3 -m src.ingest.upload_to_hdfs

run_spark() {
  local module="$1"
  shift
  compose_exec spark spark-submit --master "$SPARK_MASTER_ARG" \
    --driver-memory "$SPARK_DRIVER_MEMORY" "/workspace/${module}" "$@"
}

echo "Cleaning tiles for region $REGION..."
run_spark src/process/clean_tiles.py --region "$REGION"
echo "Assigning region geography..."
run_spark src/process/geo_join.py --region "$REGION"
echo "Engineering region features..."
run_spark src/process/feature_engineering.py --region "$REGION"
echo "Training and evaluating clusters..."
run_spark src/model/kmeans_clustering.py --region "$REGION"
echo "Serving cluster results in MongoDB..."
run_spark src/serve/mongo_writer.py --region "$REGION"
echo "Exporting region GeoJSON..."
compose_exec spark bash -lc "cd /workspace && python3 -m src.visualize.export_geojson --region $REGION"

echo "Pipeline complete for region $REGION. View the dashboard with: streamlit run src/visualize/dashboard.py"
