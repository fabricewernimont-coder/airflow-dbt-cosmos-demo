from datetime import datetime
from pathlib import Path
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Chemin vers ton projet dbt dans le conteneur Astro
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/jaffle_shop")

with DAG(
    dag_id="dbt_segment_customers",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # On retire complètement la tâche pre_cleanup
    
    dbt_branch = DbtTaskGroup(
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        operator_args={
            "full_refresh": True, # On garde le full_refresh pour que dbt gère lui-même le nettoyage des tables
        },
        profile_config=ProfileConfig(
            profile_name="jaffle_shop",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_default",
                profile_args={"schema": "public"},
            ),
        ),
        render_config=RenderConfig(
            select=["+customers"], 
        ),
    )

    # La tâche s'exécute maintenant seule, sans dépendance cassée
    dbt_branch