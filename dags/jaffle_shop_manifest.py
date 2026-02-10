from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# 1. On définit les chemins vers le projet et le manifest
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")
MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"

# 2. Ta config de connexion (identique à l'autre DAG)
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "public"},
    ),
)

# 3. Le DAG version "Manifest"
jaffle_shop_manifest = DbtDag(
    project_config=ProjectConfig(
        DBT_PROJECT_PATH,
        manifest_path=MANIFEST_PATH, # <-- C'EST LA CLÉ !
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt-env/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
    },
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="jaffle_shop_manifest",
)