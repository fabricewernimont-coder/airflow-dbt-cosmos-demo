from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator # Add this
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")

with DAG(
    dag_id="dbt_only_stg_orders",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # 1. Pre-cleanup task to remove any zombie backup relations
    cleanup_backups = PostgresOperator(
        task_id="cleanup_dbt_backups",
        postgres_conn_id="postgres_default",
        sql="DROP TABLE IF EXISTS stg_orders__dbt_backup CASCADE; DROP VIEW IF EXISTS stg_orders__dbt_backup CASCADE;"
    )

    # 2. Your dbt execution
    dbt_sniper = DbtTaskGroup(
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        operator_args={"full_refresh": True},
        profile_config=ProfileConfig(
            profile_name="jaffle_shop",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_default",
                profile_args={"schema": "public"},
            ),
        ),
        render_config=RenderConfig(
            select=["stg_orders"], 
        ),
    )

    cleanup_backups >> dbt_sniper