from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sqlite3
import requests

@dag(
    dag_id='verify_connection_test',
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def test_connection_pipeline():

    @task(task_id='test_sqlite_connection')
    def check_sqlite():
        db_path = '/opt/airflow/dags/data/sqlite-sakila.db'
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()
            conn.close()
            print(f"SUCCESS: SQLite connection verified! Test query result: {result}")
        except Exception as e:
            print(f"FAILED: Could not connect to SQLite. Error: {e}")
            raise e

    @task(task_id='test_api_connection')
    def check_api():
        url = "http://host.docker.internal:8000/api/health" # Or your target endpoint
        try:
            response = requests.get(url, timeout=5)
            print(f"SUCCESS: API responded with status code: {response.status_code}")
        except Exception as e:
            print(f"FAILED: Could not reach API. Error: {e}")
            raise e

    # Run connection checks
    check_sqlite()
    check_api()

test_connection_pipeline()