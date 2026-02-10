import os
from pathlib import Path
from pendulum import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Chemin absolu à l'intérieur du conteneur Astro
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")

# Configuration de la connexion (utilise la DB Postgres par défaut d'Astro)
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
    # Utilise l'exécutable dbt installé via ton Dockerfile ou requirements.txt
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt"
    ),
    dag_id="jaffle_shop_cosmos",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
)