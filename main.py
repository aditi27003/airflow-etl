from fastapi import FastAPI
import os
import sqlite3
import pandas as pd

app = FastAPI(title="WSL SQLite ETL API")

# Update this to your absolute or relative path where your SQLite .db file is located in WSL
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite-sakila.db")

@app.get("/api/health")
def health_check():
    """Simple health check endpoint for Airflow connection testing."""
    return {"status": "healthy", "message": "FastAPI is running successfully!"}

@app.get("/api/extract-transactions")
def get_transactions():
    """Reads data from the SQLite3 database and returns it as JSON."""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Query your target table
        query = " select * from actor limit 10;"
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Convert DataFrame to records (list of dictionaries)
        records = df.to_dict(orient="records")
        return {"status": "success", "row_count": len(records), "data": records}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
