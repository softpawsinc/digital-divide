"""Create the four checked-in notebooks from compact, reviewable cell definitions."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def md(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": source.splitlines(keepends=True)}


def code(source: str) -> dict:
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
            "source": source.splitlines(keepends=True)}


def notebook(cells: list[dict]) -> dict:
    return {"cells": cells, "metadata": {"kernelspec": {"display_name": "Python 3",
            "language": "python", "name": "python3"}, "language_info": {"name": "python",
            "version": "3.10.0"}}, "nbformat": 4, "nbformat_minor": 5}


NOTEBOOKS = {
    "01_data_exploration.ipynb": [
        md("# 1. Data exploration\n\nInspect one downloaded Ookla quarter and the companion ACS tract table. Run `python -m src.ingest.download_ookla --limit 1` and `python -m src.ingest.download_census` first.\n"),
        code("from pathlib import Path\nimport pandas as pd\nimport pyarrow.parquet as pq\n\nroot = Path('.')\nookla_files = sorted((root / 'data/ookla').rglob('*.parquet'))\ncensus_file = root / 'data/census/acs5_tract_2022.parquet'\nif not ookla_files:\n    print('No Ookla Parquet file found. Run the ingestion command above.')\nelse:\n    metadata = pq.ParquetFile(ookla_files[0]).metadata\n    print('File:', ookla_files[0], 'rows:', metadata.num_rows)\n    print(pq.read_schema(ookla_files[0]))\n    sample = pd.read_parquet(ookla_files[0]).head(10)\n    display(sample)\nif census_file.exists():\n    census = pd.read_parquet(census_file)\n    display(census.head())\n    display(census[['broadband_adoption_pct', 'median_income', 'education_bachelors_pct']].describe())\nelse:\n    print(f'Missing {census_file}; run Census ingestion.')\n"),
        md("## Notes\n\nOokla speed fields are in kilobits per second in the source and are converted to Mbps in Spark. ACS adoption is calculated as broadband-of-any-type households divided by total households; education is the share of adults 25+ with a bachelor's degree or higher.\n"),
    ],
    "02_processing.ipynb": [
        md("# 2. Processing and spatial join\n\nStart the Docker services with `docker compose -f docker/docker-compose.yml up -d --build --wait`, ingest source files, then upload them to HDFS. The commands below run the same Spark jobs as the pipeline script.\n"),
        code("import subprocess\n\nCOMPOSE = ['docker', 'compose', '-f', 'docker/docker-compose.yml']\ndef spark_job(module, *args):\n    command = 'cd /workspace && spark-submit --master local[*] /workspace/' + module + ' ' + ' '.join(args)\n    return subprocess.run(COMPOSE + ['exec', '-T', 'spark', 'bash', '-lc', command], check=True)\n\n# If needed, run once: python -m src.ingest.upload_to_hdfs inside the spark service.\nspark_job('src/process/clean_tiles.py')\nspark_job('src/process/geo_join.py', '--boundaries', '/workspace/data/census/boundaries')\nspark_job('src/process/feature_engineering.py')\n"),
        md("## Inspect the feature table\n\nThe join uses tile centroids and TIGER tract polygons. Tile download, upload, and latency averages are weighted by test count. The summed device count is the sum of Ookla's per-tile unique-device counts; the public data does not expose a cross-tile device identifier for tract-level deduplication.\n"),
        code("inspect = 'cd /workspace && hdfs dfs -ls /user/digitaldivide/processed/tract_features'\nsubprocess.run(COMPOSE + ['exec', '-T', 'spark', 'bash', '-lc', inspect], check=True)\n"),
    ],
    "03_clustering.ipynb": [
        md("# 3. Clustering and evaluation\n\nRun K-Means over the six imputed tract features. StandardScaler normalizes the features, and candidate k values are compared with WSSSE and a deterministic sample silhouette score. The highest sampled silhouette selects the final k.\n"),
        code("import json\nimport subprocess\nfrom pathlib import Path\n\ncommand = 'cd /workspace && spark-submit --master local[*] /workspace/src/model/kmeans_clustering.py'\nsubprocess.run(['docker', 'compose', '-f', 'docker/docker-compose.yml', 'exec', '-T', 'spark', 'bash', '-lc', command], check=True)\nmetrics = json.loads(Path('data/processed/model_metrics.json').read_text())\nprint('Selected k:', metrics['selected_k'])\nprint(metrics['metrics'])\n"),
        code("from IPython.display import Image, display\nplot = Path('data/processed/plots/kmeans_evaluation.png')\nif plot.exists():\n    display(Image(filename=str(plot)))\n"),
        md("## Interpret clusters\n\nSegment names are assigned from cluster-level broadband adoption, download speed, and income compared with overall medians. They are descriptive shorthand; they are not causal conclusions. Review centroids and feature distributions before using them for policy decisions.\n"),
    ],
    "04_visualization.ipynb": [
        md("# 4. Serving and visualization\n\nAfter running the pipeline, cluster rows are stored in MongoDB and joined to TIGER tract boundaries in a local GeoJSON file. The Streamlit app provides filters and interactive maps.\n"),
        code("from pathlib import Path\nimport pandas as pd\n\ngeojson_path = Path('data/processed/clusters.geojson')\nif geojson_path.exists():\n    import geopandas as gpd\n    tracts = gpd.read_file(geojson_path)\n    print(tracts.shape, tracts.crs)\n    display(tracts.drop(columns='geometry').head())\nelse:\n    print('Missing map export. Run python -m src.visualize.export_geojson after MongoDB is populated.')\n"),
        code("import subprocess\n\n# Start the interactive app in a terminal from the project root.\nprint('streamlit run src/visualize/dashboard.py')\n"),
        md("## Dashboard guide\n\nSelect state FIPS, segment, and household-income range in the sidebar. The map and tables respond to these filters. Summary panels show cluster size, speed, and adoption. MongoDB is expected at `localhost:27017` when the app runs on your laptop.\n"),
    ],
}


def main() -> None:
    destination = ROOT / "notebooks"
    destination.mkdir(parents=True, exist_ok=True)
    for name, cells in NOTEBOOKS.items():
        (destination / name).write_text(json.dumps(notebook(cells), indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
