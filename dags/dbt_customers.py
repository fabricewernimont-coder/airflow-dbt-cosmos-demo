from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Path to your dbt project within the Astro container
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")

with DAG(
    dag_id="dbt_segment_customers",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # Pre-cleanup: Drops potential zombie relations for the customers branch
    # This ensures your demo never gets stuck on "relation already exists"
    pre_cleanup = PostgresOperator(
        task_id="pre_dbt_cleanup",
        postgres_conn_id="postgres_default",
        sql="""
            DROP VIEW IF EXISTS customers, stg_customers, stg_orders, raw_customers, raw_orders, raw_payments CASCADE;
            DROP TABLE IF EXISTS customers, stg_customers, stg_orders, raw_customers, raw_orders, raw_payments CASCADE;
            DROP TABLE IF EXISTS customers__dbt_backup, stg_customers__dbt_backup, stg_orders__dbt_backup CASCADE;
        """
    )

    dbt_branch = DbtTaskGroup(
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        operator_args={
            "full_refresh": True, 
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
            select=["+customers"], 
        ),
    )

    pre_cleanup >> dbt_branch