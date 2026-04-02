import os
from pathlib import Path
from pendulum import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import InvocationMode
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
        dbt_executable_path=f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt",
        # ✅ Force subprocess invocation — required for deferrable watcher mode.
        # Without this, Cosmos auto-detects dbtRunner (in-process) which bypasses
        # the subprocess watcher and silently ignores deferrable=True.
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    render_config=RenderConfig(
        select=["path:models", "path:seeds"]
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": True,
        # ✅ Enables deferrable watcher mode — task defers after subprocess launch,
        # freeing the worker slot while dbt runs. Triggerer polls and resumes.
        "deferrable": True,
        # ✅ How often the Triggerer checks if the subprocess finished.
        # 5s is fine for fast models; increase to 30-60s for long warehouse transforms.
        "poll_interval": 5,
    },
    dag_id="jaffle_shop_cosmos_deferrable",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)