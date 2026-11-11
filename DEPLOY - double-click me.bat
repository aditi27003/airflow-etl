@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -Command "& .\deploy.ps1 2>&1 | Tee-Object -FilePath deploy_log.txt"
echo.
pause
