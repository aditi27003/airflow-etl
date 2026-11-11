from datetime import datetime, timedelta
import os
import sqlite3
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from airflow.decorators import dag, task

default_args = {
    'owner': 'student_instructor',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def get_s3_target():
    """Real AWS S3 if real keys are in .env, otherwise the local S3 emulator (moto) container."""
    key_id = os.getenv('AWS_ACCESS_KEY_ID', '')
    if key_id and not key_id.startswith('your_'):
        client = boto3.client(
            's3',
            aws_access_key_id=key_id,
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
        )
        return client, os.getenv('AWS_S3_BUCKET_NAME'), 'AWS S3'
    client = boto3.client(
        's3',
        endpoint_url='http://s3local:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1',
    )
    bucket = 'etl-local'
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)
    return client, bucket, 'local S3 emulator (moto)'


@dag(
    dag_id='student_sqlite_to_aws_s3_etl',
    default_args=default_args,
    description='ETL Pipeline: SQLite Source -> Pandas Transformation -> AWS S3 (or local S3 emulator) Bucket',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['education', 'etl', 'sqlite', 'aws_s3'],
)
def sqlite_to_aws_s3_pipeline():

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

    @task(task_id='upload_to_aws_s3')
    def upload_to_s3(transformed_records: list):
        if not transformed_records:
            return
        df_load = pd.DataFrame(transformed_records)
        file_name = f"customers_clean_{datetime.now().strftime('%Y-%m-%d')}.csv"
        temp_local_path = f"/tmp/{file_name}"
        df_load.to_csv(temp_local_path, index=False)
        s3_key = f"etl_output/{file_name}"

        s3_client, bucket, target = get_s3_target()
        s3_client.upload_file(temp_local_path, bucket, s3_key)
        print(f"SUCCESS: Uploaded {len(df_load)} rows to s3://{bucket}/{s3_key} on {target}")

    upload_to_s3(transform(extract()))


sqlite_to_aws_s3_pipeline()
