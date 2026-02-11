import os
import subprocess
from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# 1. Define paths and configuration
# Use absolute paths within the Docker container
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")
MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"
DBT_EXECUTABLE = Path("/usr/local/airflow/dbt_venv/bin/dbt")

# --- AUTOMATIC MANIFEST GENERATION ---
# This block ensures that the 'manifest.json' exists before Cosmos tries to load it.
# This makes the project "clonable" for others without manual dbt commands.
if not MANIFEST_PATH.exists():
    print(f"Manifest not found at {MANIFEST_PATH}. Generating it now...")
    
    # Ensure the target directory exists (safety net for the .gitkeep strategy)
    os.makedirs(MANIFEST_PATH.parent, exist_ok=True)
    
    # Run 'dbt parse' using the virtual environment created in the Dockerfile.
    # We point to the internal 'etc' folder for profiles.
    subprocess.run([
        str(DBT_EXECUTABLE), 
        "parse", 
        "--project-dir", str(DBT_PROJECT_PATH), 
        "--profiles-dir", str(DBT_PROJECT_PATH / "etc")
    ], check=False)
# -------------------------------------

# 2. Connection configuration
# Mapping Airflow's 'postgres_default' to dbt's profile requirements
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "public"},
    ),
)

# 3. The Cosmos DbtDag 
# This automatically turns dbt models into an Airflow Graph
jaffle_shop_manifest = DbtDag(
    project_config=ProjectConfig(
        DBT_PROJECT_PATH,
        manifest_path=MANIFEST_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=str(DBT_EXECUTABLE),
    ),
    operator_args={
        "install_deps": True, # Ensures dbt packages are installed
        "full_refresh": True, # Useful for dev environments to reset tables
    },
    schedule="@daily",  
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="jaffle_shop_manifest",
)