import os
from pathlib import Path
from pendulum import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.sdk import Asset, task

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

pipeline_done = Asset("astro://jaffle_shop/pipeline/complete")

jaffle_shop_cosmos = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt"
    ),
    render_config=RenderConfig(
        select=["path:models", "path:seeds"]
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    dag_id="jaffle_shop_cosmos",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)

# Terminal task emits the pipeline_done asset after all models complete
with jaffle_shop_cosmos:
    @task(outlets=[pipeline_done])
    def emit_pipeline_done():
        print("All dbt models completed successfully — emitting pipeline asset.")

    emit_pipeline_done()