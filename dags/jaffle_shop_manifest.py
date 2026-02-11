"""
IMPORTANT: This DAG requires a 'manifest.json' file to be present in the dbt project's 
target directory to be fully operational. 

If you are cloning this repository for the first time:
1. Ensure you have run 'dbt parse' within your dbt project or through 
   the 'astro dev bash' command to generate the manifest.
2. The DAG is wrapped in a try/except block to prevent Airflow from crashing 
   if the manifest is missing, but the dbt graph will only appear once the 
   file is generated.
"""

import os
from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# 1. Define paths
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")
MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"
DBT_EXECUTABLE = Path("/usr/local/airflow/dbt_venv/bin/dbt")

# 2. Connection configuration
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "public"},
    ),
)

# 3. Safe DAG initialization
# We use a try/except block so Airflow doesn't break if the manifest is missing
try:
    jaffle_shop_manifest = DbtDag(
        project_config=ProjectConfig(
            DBT_PROJECT_PATH,
            manifest_path=MANIFEST_PATH if MANIFEST_PATH.exists() else None,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=str(DBT_EXECUTABLE),
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": True,
        },
        schedule="@daily",  
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id="jaffle_shop_manifest",
    )
except Exception as e:
    # This allows Airflow to load the file even if Cosmos fails
    print(f"Cosmos could not load the dbt project: {e}")