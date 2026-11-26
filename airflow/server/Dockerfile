# Airflow plus uv. The DAGs' tasks run the pipeline with `uv run` in the project's own venv, which uv
# builds on first use in /opt/state/venv (see docker-compose.yml), so Airflow's pins never meet dlt's.
FROM apache/airflow:3.3.2-python3.12
COPY --from=ghcr.io/astral-sh/uv:0.13.9 /uv /usr/local/bin/uv
