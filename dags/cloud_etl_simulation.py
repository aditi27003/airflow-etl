from datetime import datetime, timedelta
import os
import sqlite3
import pandas as pd
from airflow.decorators import dag, task

default_args = {
    'owner': 'student_instructor',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='student_sqlite_to_cloud_etl',
    default_args=default_args,
    description='Educational ETL Pipeline: SQLite Source -> Pandas Transformation -> Simulated Cloud Storage Sink',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['education', 'etl', 'sqlite', 'cloud_simulation'],
)
def sqlite_to_cloud_pipeline():

    # 1. Source Configuration: Path to SQLite database inside the Airflow Docker container
    SQLITE_DB_PATH = '/opt/airflow/dags/data/sqlite-sakila.db'
    
    # 2. Target Configuration: Simulating a cloud object storage bucket (e.g., AWS S3 / GCS bucket folder)
    SIMULATED_CLOUD_BUCKET = '/opt/airflow/dags/cloud_bucket'

    @task(task_id='extract_from_sqlite')
    def extract() -> list:
        """
        EXTRACT STAGE:
        Connects to the source SQLite database file and extracts raw customer records.
        """
        print(f"Connecting to SQLite source database at: {SQLITE_DB_PATH}")
        
        # Standard Python sqlite3 connection
        conn = sqlite3.connect(SQLITE_DB_PATH)
        try:
            # Extract sample customer records from the Sakila database
            query = "SELECT customer_id, store_id, first_name, last_name, email, active FROM customer LIMIT 50;"
            df = pd.read_sql(query, conn)
            
            print(f"Successfully extracted {len(df)} raw records from SQLite source.")
            
            # Convert DataFrame to dictionary list so Airflow can pass it via XComs
            return df.to_dict(orient='records')
        finally:
            conn.close()

    @task(task_id='transform_data')
    def transform(raw_records: list) -> list:
        """
        TRANSFORM STAGE:
        Cleans and processes raw data using Pandas (mimicking distributed cloud transformation):
        - Normalizes name cases for data quality hygiene
        - Injects audit metadata timestamps for data governance
        """
        if not raw_records:
            print("No records found to transform.")
            return []

        df = pd.DataFrame(raw_records)
        print(f"Starting transformation on {len(df)} records...")

        # 1. Data Cleaning: Capitalize customer names properly
        df['first_name'] = df['first_name'].str.capitalize()
        df['last_name'] = df['last_name'].str.capitalize()

        # 2. Data Enrichment: Add cloud ingestion audit metadata timestamps
        df['etl_loaded_at'] = datetime.utcnow().isoformat()
        df['pipeline_source'] = 'sqlite_sakila_db'

        print("Transformation complete. Data sample preview:")
        print(df.head(3))

        return df.to_dict(orient='records')

    @task(task_id='load_to_cloud_bucket')
    def load(transformed_records: list):
        """
        LOAD STAGE:
        Simulates writing processed data to Cloud Object Storage (like AWS S3 or Google Cloud Storage)
        by saving partitioned CSV/Parquet files into a designated cloud bucket folder.
        """
        if not transformed_records:
            print("No transformed records to load.")
            return

        df_load = pd.DataFrame(transformed_records)
        
        # Ensure the simulated cloud bucket directory exists
        os.makedirs(SIMULATED_CLOUD_BUCKET, exist_ok=True)

        # Generate a partition file name mimicking cloud data lake partitioning
        date_str = datetime.now().strftime('%Y-%m-%d')
        file_name = f"customers_clean_{date_str}.csv"
        file_path = os.path.join(SIMULATED_CLOUD_BUCKET, file_name)

        # Save dataset to the simulated cloud storage path
        df_load.to_csv(file_path, index=False)
        print(f"SUCCESS: Uploaded {len(df_load)} rows to simulated cloud bucket at: {file_path}")

    # --- Pipeline Task Orchestration Flow ---
    raw_data = extract()
    clean_data = transform(raw_data)
    load(clean_data)

# Instantiate the DAG
dag_instance = sqlite_to_cloud_pipeline()