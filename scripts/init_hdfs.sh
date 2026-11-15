#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$PROJECT_ROOT/docker/docker-compose.yml")
cd "$PROJECT_ROOT"
BASE_PATH="${HDFS_BASE_PATH:-$(python -c 'from src.config import load_config; print(load_config()["hdfs"]["base_path"])')}"

# HDFS root is owned by the NameNode superuser. Bootstrap the application tree
# there, then grant ownership to the Spark service account instead of making
# the whole tree world-writable.
"${COMPOSE[@]}" exec -T -u root namenode hdfs dfs -mkdir -p \
  "$BASE_PATH/raw/ookla" "$BASE_PATH/raw/census" "$BASE_PATH/processed" "$BASE_PATH/models"
"${COMPOSE[@]}" exec -T -u root namenode hdfs dfs -chown -R spark:supergroup "$BASE_PATH"
"${COMPOSE[@]}" exec -T -u root namenode hdfs dfs -ls -R "$BASE_PATH"
