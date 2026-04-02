import os
from pathlib import Path
from pendulum import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Absolute path inside the Astro container
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")

# Connection configuration (uses the default Astro Postgres DB)
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "public"},
    )
)

jaffle_shop_cosmos = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    # Isolation: Uses the dbt virtual environment built in the Dockerfile
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt"
    ),
    # RenderConfig ensures that both seeds (CSVs) and models are included in the DAG
    render_config=RenderConfig(
        select=["path:models", "path:seeds"]
    ),
    # Added install_deps to ensure dbt packages are ready
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    dag_id="jaffle_shop_cosmos",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)