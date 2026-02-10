from datetime import datetime
from pathlib import Path
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Chemin vers ton projet dbt dans le conteneur Astro
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")

with DAG(
    dag_id="dbt_segment_customers",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    dbt_branch = DbtTaskGroup(
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        operator_args={
            "full_refresh": True, # Force le nettoyage des tables de backup existantes
        },
        profile_config=ProfileConfig(
            profile_name="jaffle_shop",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_default",
                profile_args={"schema": "public"},
            ),
        ),
        render_config=RenderConfig(
            # On se concentre uniquement sur la branche customers
            select=["+customers"], 
        ),
    )