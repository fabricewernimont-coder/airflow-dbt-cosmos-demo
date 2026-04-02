import os
from pathlib import Path
from pendulum import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode
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

jaffle_shop_cosmos_deferrable = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        # ✅ WATCHER mode: launches dbt subprocess then defers to Triggerer.
        # This is the correct API — deferrable=True in operator_args is a no-op.
        execution_mode=ExecutionMode.WATCHER,
        dbt_executable_path=f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt",
    ),
    render_config=RenderConfig(
        select=["path:models", "path:seeds"]
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    dag_id="jaffle_shop_cosmos_deferrable",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)