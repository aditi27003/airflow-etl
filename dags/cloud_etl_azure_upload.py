from datetime import datetime, timedelta
import os
import sqlite3
import pandas as pd
from airflow.decorators import dag, task

default_args = {
    'owner': 'student_instructor',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Local Azurite emulator (runs as the "azurite" container in docker-compose).
# Used automatically until a real AZURE_STORAGE_CONNECTION_STRING is put in .env
AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devaccount;"
    "AccountKey=YWlyZmxvdy1ldGwtbG9jYWwtZGV2LWtleS0yMDI2;"
    "BlobEndpoint=http://azurite:10000/devaccount;"
)


def get_azure_target():
    """Real Azure Blob Storage if a real connection string is in .env, otherwise local Azurite."""
    from azure.storage.blob import BlobServiceClient

    conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
    container = os.getenv('AZURE_CONTAINER_NAME', 'etl-output') or 'etl-output'
    if 'AccountName=' in conn_str or 'BlobEndpoint=' in conn_str:
        target = 'Azure Blob Storage'
    else:
        conn_str = os.getenv('AZURITE_CONNECTION_STRING', AZURITE_CONNECTION_STRING)
        target = 'local Azurite emulator'

    service = BlobServiceClient.from_connection_string(conn_str)
    container_client = service.get_container_client(container)
    if not container_client.exists():
        container_client.create_container()
    return container_client, container, target


@dag(
    dag_id='student_sqlite_to_azure_blob_etl',
    default_args=default_args,
    description='ETL Pipeline: SQLite Source -> Pandas Transformation -> Azure Blob Storage',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['education', 'etl', 'sqlite', 'azure'],
)
def sqlite_to_azure_blob_pipeline():

    SQLITE_DB_PATH = '/opt/airflow/dags/data/sqlite-sakila.db'

    @task(task_id='extract_from_sqlite')
    def extract() -> list:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        try:
            query = "SELECT customer_id, store_id, first_name, last_name, email, active FROM customer LIMIT 50;"
            df = pd.read_sql(query, conn)
            print(f"Extracted {len(df)} rows from SQLite")
            return df.to_dict(orient='records')
        finally:
            conn.close()

    @task(task_id='transform_data')
    def transform(raw_records: list) -> list:
        if not raw_records:
            return []
        df = pd.DataFrame(raw_records)
        df['first_name'] = df['first_name'].str.capitalize()
        df['last_name'] = df['last_name'].str.capitalize()
        df['etl_loaded_at'] = datetime.utcnow().isoformat()
        df['pipeline_source'] = 'sqlite_sakila_db'
        return df.to_dict(orient='records')

    @task(task_id='upload_to_azure_blob')
    def upload_to_azure(transformed_records: list):
        if not transformed_records:
            return
        df_load = pd.DataFrame(transformed_records)
        blob_name = f"etl_output/customers_clean_{datetime.now().strftime('%Y-%m-%d')}.csv"
        csv_bytes = df_load.to_csv(index=False).encode('utf-8')

        container_client, container, target = get_azure_target()
        container_client.upload_blob(name=blob_name, data=csv_bytes, overwrite=True)
        print(f"SUCCESS: Uploaded {len(df_load)} rows to {container}/{blob_name} on {target}")

    upload_to_azure(transform(extract()))


sqlite_to_azure_blob_pipeline()
