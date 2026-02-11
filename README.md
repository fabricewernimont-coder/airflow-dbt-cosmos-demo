# 🚀 Astro CLI + dbt Cosmos: Jaffle Shop Industrial Demo

This project demonstrates a professional-grade integration between **Astro CLI (Apache Airflow)** and **dbt Cosmos**. It showcases how to bridge the gap between Analytics Engineering and Data Orchestration.

## 🏗️ Project Architecture

The repository is structured to separate data transformations (dbt) from orchestration (DAGs). This structure also allows to isolate CI/CD branches accordgingly :
* **dags/**: Airflow DAG definitions using Cosmos `DbtDag` and `DbtTaskGroup`.
* **dbt/jaffle_shop/**: The core dbt project (models, seeds, tests).
* **docker-compose.override.yml**: Enables real-time sync between your host machine and the Docker containers.

---

## 🧭 DAGs Catalog

This project implements different orchestration strategies to suit various needs:

### 1. jaffle_shop_cosmos (Dynamic Mode)
* **How it works**: Scans the `models/` folder at runtime.
* **Best for**: Rapid development. Any SQL change is reflected instantly in Airflow.

### 2. jaffle_shop_manifest (Production Mode)
* **How it works**: Points to `target/manifest.json`.
* **Best for**: Production environments. It provides faster parsing and absolute consistency.

### 3. Granular Control on dbt & Data-Awareness
* **dbt_segment_customers & dbt_only_stg_orders**: Demonstrate how to isolate and run specific dbt models or groups.
* **downstream_asset_check**: Showcases **Data-Aware Scheduling**. This DAG triggers automatically when the postgres `customers` table is updated airflow assets.

---

## 💻 Local Environment Setup

To maintain a clean workflow, we distinguish between the **Airflow Runtime** (Docker) and the **Development Tools** (Local Python).

### 1. Airflow Environment Configuration (Mandatory)
To automate the connection to the internal Postgres database, you need to add the connection string to your .env file. This avoids manual configuration in the Airflow UI and ensures the environment is "Plug & Play".

* Open the .env file at the root of the project (created by Astro CLI).
* Append the following lines:
```bash
# Airflow Connection for dbt Cosmos
AIRFLOW_CONN_POSTGRES_DEFAULT='postgres://postgres:postgres@postgres:5432/postgres'
```

### 2. dbt Virtual Environment
Ensure you have Python 3.10+ installed.

```bash
# Create and activate the environment
python3 -m venv dbt-env
source dbt-env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Essential dbt Commands
From within `dbt-env`, use these commands to manage your project:

* **Seed**: `dbt seed --profiles-dir etc` (Uploads CSVs to Postgres)
* **Debug**: `dbt debug --profiles-dir etc` (Tests DB connectivity)
* **Compile**: `dbt compile --profiles-dir etc` (Generates the `manifest.json`)

---

## 🛠️ Requirements & Dependencies
The project relies on two main requirement files:

1. **requirements.txt** (Local & Airflow):
    * `astronomer-cosmos`: The magic link between dbt and Airflow.
    * `dbt-postgres`: The adapter for Postgres communication.
2. **packages.txt** (OS Level):
    * Used by Astro CLI to install system-level dependencies like `libpq-dev`.

---

## 📝 VS Code Setup (Pro Tips)
Recommended extensions:

* **dbt Power User**: For "Go to Definition" and model visualization.
* **SQLFluff**: For automated SQL linting.
* **Docker**: To monitor containers directly from VS Code.

**Postgres Connection (DB Client):**

* **Host**: `localhost`
* **Port**: `5432`
* **User/Pass**: `postgres` / `postgres`

---

## 🚀 Launching the Stack
Ready to go? Start your engine ! :

```bash
astro dev start
```