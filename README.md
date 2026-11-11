# SQLite → Airflow → Azure Blob ETL Pipeline

An end-to-end ETL pipeline built with **Apache Airflow 3** running in **Docker**. It extracts customer data from a SQLite database (Sakila sample DB), cleans it with **Pandas**, and loads it into **Azure Blob Storage** — or into a local Azurite emulator when no Azure account is configured.

## Architecture

```
SQLite (sakila.db) ──► Extract ──► Transform (Pandas) ──► Load ──► Azure Blob Storage
                                                              └──► Local "cloud bucket" folder
```

## DAGs

| DAG | What it does |
|---|---|
| `student_sqlite_to_azure_blob_etl` | Extracts customers from SQLite, normalises names, adds audit columns, uploads a dated CSV to Azure Blob Storage (or Azurite) |
| `student_sqlite_to_cloud_etl` | Same pipeline, writing partitioned CSVs to a simulated cloud bucket (`dags/cloud_bucket/`) |
| `verify_connection_test` | Manual check that the SQLite DB and the FastAPI service are reachable |

`extras/cloud_etl_s3_upload.py` is an AWS S3 version of the same pipeline.

## Tech stack

Apache Airflow 3 (TaskFlow API) · Docker Compose · PostgreSQL · Redis · Pandas · SQLite · Azure Blob Storage / Azurite · FastAPI · Adminer

## Project structure

```
dags/                       Airflow DAGs + source DB (dags/data/sqlite-sakila.db)
dags/cloud_bucket/          Sample output from the simulated-cloud DAG
extras/                     AWS S3 variant, FastAPI launcher without Docker
main.py                     FastAPI service (health check + extract endpoint)
docker-compose.yaml         Airflow, Postgres, Redis, Adminer, FastAPI, Azurite
deploy.ps1                  One-click build, start and trigger script (Windows)
save_azure_key.ps1          Saves an Azure connection string from the clipboard into .env
```

## How to run (Windows + Docker Desktop)

1. Install and start Docker Desktop.
2. Copy `.env.example` to `.env` (or let `deploy.ps1` create it).
3. Double-click **`DEPLOY - double-click me.bat`**. It initialises Airflow, starts every service and triggers the DAGs.
4. Open:
   - Airflow UI: http://localhost:8080 (`airflow` / `airflow`)
   - Adminer: http://localhost:8081
   - FastAPI: http://localhost:8000/api/health

### Using real Azure Blob Storage

Copy your storage account connection string (Azure portal → Access keys), then double-click **`SAVE AZURE KEY - double-click me.bat`**. Without it, the pipeline uploads to the local Azurite emulator.

## Author

Aditi Srivastava — MBA, Graphic Era (Deemed to be University)
