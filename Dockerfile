FROM astrocrpublic.azurecr.io/runtime:3.1-12

USER root

# Install system dependencies for Postgres
RUN apt-get update && apt-get install -y libpq-dev

# Create directory and copy dbt project
RUN mkdir -p /usr/local/airflow/dbt
COPY dbt /usr/local/airflow/dbt

# Setup dbt virtual environment
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-postgres
USER astro