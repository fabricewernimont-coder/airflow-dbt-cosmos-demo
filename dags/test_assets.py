from airflow.sdk import dag, Asset, task
from pendulum import datetime

# L'URI générée par Cosmos dans Airflow 3 utilise des slashes
my_asset = Asset("postgres://postgres:5432/postgres/public/customers")

@dag(
    schedule=[my_asset],  
    start_date=datetime(2025, 1, 1),
    catchup=False
)
def downstream_asset_check():
    @task
    def print_msg():
        print("L'Asset dbt est prêt, je peux lancer la suite !")
    
    print_msg()

downstream_asset_check()