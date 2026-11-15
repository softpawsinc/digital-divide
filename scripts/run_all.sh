#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Make host-side Python commands use the project environment when it exists.
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  for candidate in "$PROJECT_ROOT/.venv311" "$PROJECT_ROOT/.venv"; do
    if [[ -x "$candidate/bin/python" ]]; then
      export PATH="$candidate/bin:$PATH"
      break
    fi
  done
fi

COMPOSE=(docker compose -f docker/docker-compose.yml)
REGIONS="${REGIONS:-us india world}"
REGIONS="${REGIONS//,/ }"

read -r -a region_list <<< "$REGIONS"
if [[ "${#region_list[@]}" -eq 0 ]]; then
  echo "REGIONS must contain at least one of: us india world." >&2
  exit 2
fi
for region in "${region_list[@]}"; do
  case "$region" in
    us|india|world) ;;
    *) echo "Unknown region '$region'; expected us, india, or world." >&2; exit 2 ;;
  esac
done

echo "Starting HDFS, MongoDB, and the Spark runtime..."
"${COMPOSE[@]}" up -d --build --wait

if [[ "${SKIP_PIPELINES:-0}" == "1" ]]; then
  echo "Skipping region pipelines (SKIP_PIPELINES=1)."
else
  for region in "${region_list[@]}"; do
    echo "Running the $region pipeline..."
    SKIP_DOCKER_UP=1 REGION="$region" "$PROJECT_ROOT/scripts/run_pipeline.sh"
  done
fi

if [[ "${NO_DASHBOARD:-0}" == "1" ]]; then
  echo "Skipping dashboard (NO_DASHBOARD=1)."
else
  echo "Launching the dashboard. Use its sidebar to switch tiers."
  streamlit run src/visualize/dashboard.py
fi
