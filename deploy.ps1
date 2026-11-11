# One-click build & deploy for the Airflow 3 ETL lab (Windows / Docker Desktop)
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot

if (-not (Test-Path ".env")) {
  Write-Host "Creating .env ..." -ForegroundColor Cyan
  @"
AIRFLOW_UID=50000
FERNET_KEY=-yFIUkj2WZbE2zeZo4PF8-8AtQUeEXMnn7OxBiAAeCQ=
AIRFLOW__CORE__LOAD_EXAMPLES=false
AZURE_STORAGE_CONNECTION_STRING=your_azure_connection_string
AZURE_CONTAINER_NAME=etl-output
"@ | Set-Content -Path ".env" -Encoding ascii
}

Write-Host "`n[1/6] Checking Docker..." -ForegroundColor Cyan
docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "Docker Desktop is not running. Start it and re-run." -ForegroundColor Red; exit 1 }

Write-Host "[2/6] Stopping any old Airflow stack using port 8080..." -ForegroundColor Cyan
$busy = docker ps --filter "publish=8080" -q
if ($busy) { Write-Host "Port 8080 is used by container(s): $busy - stopping them"; docker stop $busy | Out-Null }

Write-Host "[3/6] Initialising Airflow DB (first run pulls images, 3-5 min)..." -ForegroundColor Cyan
docker compose up airflow-init
if ($LASTEXITCODE -ne 0) { Write-Host "airflow-init failed - see output above." -ForegroundColor Red; exit 1 }

Write-Host "[4/6] Starting all services..." -ForegroundColor Cyan
docker compose up -d
if ($LASTEXITCODE -ne 0) { exit 1 }
# airflow-init is a one-time setup job; remove it after it finishes so Docker Desktop shows all containers green
docker compose rm -f airflow-init | Out-Null

Write-Host "[5/6] FastAPI runs as the fastapi container on port 8000" -ForegroundColor Cyan

Write-Host "[6/6] Waiting for Airflow API server to become healthy..." -ForegroundColor Cyan
for ($i = 0; $i -lt 60; $i++) {
  try { $r = Invoke-WebRequest -UseBasicParsing http://localhost:8080/api/v2/monitor/health -TimeoutSec 5; if ($r.StatusCode -eq 200) { break } } catch {}
  Start-Sleep -Seconds 5
}
Start-Sleep -Seconds 20   # give the dag-processor time to parse the DAGs

Write-Host "`nDAGs found:" -ForegroundColor Cyan
docker compose exec airflow-scheduler airflow dags list

Write-Host "`nTriggering connection test + local ETL DAGs..." -ForegroundColor Cyan
foreach ($d in "verify_connection_test","student_sqlite_to_cloud_etl","student_sqlite_to_azure_blob_etl") {
  docker compose exec airflow-scheduler airflow dags unpause $d
  docker compose exec airflow-scheduler airflow dags trigger $d
}

Write-Host "`nDONE." -ForegroundColor Green
Write-Host "  Airflow UI : http://localhost:8080  (airflow / airflow)"
Write-Host "  Adminer    : http://localhost:8081  (PostgreSQL / postgres / airflow / airflow / airflow)"
Write-Host "  FastAPI    : http://localhost:8000/api/health"
Write-Host "  Local ETL output appears in dags\cloud_bucket\ after the run finishes."
Write-Host "  Local Azure Blob (Azurite): localhost:10000 - container etl-output"
Write-Host "  For real Azure: add AZURE_STORAGE_CONNECTION_STRING=... to .env and run this script again."
