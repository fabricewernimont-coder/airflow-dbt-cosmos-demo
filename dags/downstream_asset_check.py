from airflow.sdk import dag, Asset, task
from pendulum import datetime

# The URI of the asset
# represents the whole dbt model 
my_asset = Asset("astro://jaffle_shop/pipeline/complete")

@dag(
    schedule=[my_asset],  # This DAG triggers automatically when the whole dbt model is updated
    start_date=datetime(2025, 1, 1),
    catchup=False
)
def downstream_asset_check():
    @task
    def print_msg():
        # Translated print message for the team
        print("The dbt model has been executed, I can now update my end user report!")
    
    print_msg()

downstream_asset_check()