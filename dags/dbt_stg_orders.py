from datetime import datetime
from pathlib import Path
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Path to your dbt project
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")

with DAG(
    dag_id="dbt_only_stg_orders",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    dbt_sniper = DbtTaskGroup(
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        operator_args={
            "full_refresh": True, # Force refresh to ensure a clean state
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
            # Precise selection: just this specific model, no parents or children
            select=["stg_orders"], 
        ),
    )