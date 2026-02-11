FROM astrocrpublic.azurecr.io/runtime:3.1-12

USER root

# 1. Create the venv using direct paths (more reliable in Docker)
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres

# 2. Copy the project
COPY dbt /usr/local/airflow/dbt

# 3. Generate the manifest (The fix for your parsing errors)
# Generate the manifest without connecting to a live database
RUN cd /usr/local/airflow/dbt/jaffle_shop && \
    /usr/local/airflow/dbt_venv/bin/dbt compile --parse-only

# 4. Give ownership back to the 'astro' user
RUN chown -R astro:astro /usr/local/airflow/dbt_venv /usr/local/airflow/dbt

USER astro 