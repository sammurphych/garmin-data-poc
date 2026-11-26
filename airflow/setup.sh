#!/bin/sh
# Install Airflow into airflow/.venv, apart from the project's own venv. Airflow pins hundreds of packages,
# so it's installed against its published constraints file, and its tasks run the pipeline in the
# project's venv with `uv run` instead of importing it.
set -e
cd "$(dirname "$0")"
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION=3.12
# Two pins in the constraints file have no Intel-Mac wheels, and building them needs Rust: cryptography 50
# (Airflow accepts >=44.0.3, so the project's 48.0.1 is used; see pyproject.toml) and libcst 1.9 (Airflow
# accepts >=1.8.2; 1.8.6 is the last with Intel-Mac wheels).
curl -fsSL "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
  | sed -e 's/^cryptography==.*/cryptography==48.0.1/' -e 's/^libcst==.*/libcst==1.8.6/' > .constraints.txt
uv venv --clear --python "$PYTHON_VERSION" .venv
uv pip install --python .venv "apache-airflow==${AIRFLOW_VERSION}" --constraint .constraints.txt

# Airflow's metadata database (SQLite in airflow/home locally), and the one-slot pools that stop two DAGs
# loading the same source at once (see dags/health_common.py).
./run.sh db migrate
./run.sh pools set garmin_api 1 "One Garmin load or check at a time"
./run.sh pools set withings_api 1 "One Withings login at a time: it rotates the refresh token"
