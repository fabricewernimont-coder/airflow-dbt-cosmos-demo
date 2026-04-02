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

example_dbt_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt",
        # ✅ Switch to LocalExecutionMode.VIRTUALENV if you want process isolation,
        # but for deferrable watcher mode, the default (LOCAL) works fine here.
    ),
    render_config=RenderConfig(
        select=["path:models", "path:seeds"]
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": True,
        # ✅ KEY: enables deferrable watcher mode
        # The task submits the dbt process, then defers — freeing the worker slot
        # while dbt runs. The Triggerer polls and resumes when dbt finishes.
        "deferrable": True,
        # ✅ Polling interval for the Triggerer (seconds). 
        # Default is 5s — tune up if dbt models run long (e.g. 30s).
        "poll_interval": 5,
    },
    dag_id="jaffle_shop_cosmos",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)