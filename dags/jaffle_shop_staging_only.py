import os
from pathlib import Path
from pendulum import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "public"},
    )
)

# DAG that only runs models tagged "staging"
jaffle_shop_staging = DbtDag(
    dag_id="jaffle_shop_staging_only",
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt"
    ),
    render_config=RenderConfig(
        select=["tag:staging"],       # ← only models with tag: staging
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)