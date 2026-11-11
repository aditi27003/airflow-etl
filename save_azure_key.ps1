# Reads the Azure connection string from the clipboard, writes a clean .env, then redeploys.
Set-Location -Path $PSScriptRoot
$cs = (Get-Clipboard -Raw)
if ($cs) { $cs = $cs.Trim() }
if (-not $cs -or $cs -notmatch '^DefaultEndpointsProtocol=.*AccountName=.*AccountKey=') {
  Write-Host ""
  Write-Host "The clipboard does not contain an Azure connection string." -ForegroundColor Red
  Write-Host "In the Azure portal > Access keys > key1 > Connection string: click Show, then the copy icon, then run this again."
  Write-Host ""
  pause
  exit 1
}
$account = ([regex]::Match($cs, 'AccountName=([^;]+)')).Groups[1].Value
@"
AIRFLOW_UID=50000
FERNET_KEY=-yFIUkj2WZbE2zeZo4PF8-8AtQUeEXMnn7OxBiAAeCQ=
AZURE_STORAGE_CONNECTION_STRING=$cs
AZURE_CONTAINER_NAME=etl-output
"@ | Set-Content -Path ".env" -Encoding ascii
Write-Host "Saved connection string for storage account '$account' to .env" -ForegroundColor Green
Write-Host "Now redeploying..." -ForegroundColor Cyan
& .\deploy.ps1 2>&1 | Tee-Object -FilePath deploy_log.txt
