#!/bin/sh
# Run an Airflow command with this project's settings, e.g.
#   airflow/run.sh standalone                    # scheduler + UI on http://localhost:8080
#   airflow/run.sh dags test health_daily        # one full run in the terminal, no scheduler needed
#   airflow/run.sh tasks test health_daily extract_weather   # one task
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
export AIRFLOW_HOME="$DIR/home"                   # config, SQLite metadata db and logs (gitignored)
export AIRFLOW__CORE__DAGS_FOLDER="$DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=utc
# Local only: no login for the UI. A server needs a real login instead.
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
# macOS: Airflow forks worker processes, and forked processes crash on the system proxy lookup and on
# Objective-C initialisation without these.
export no_proxy='*'
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
exec "$DIR/.venv/bin/airflow" "$@"
