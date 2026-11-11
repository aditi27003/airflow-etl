@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File save_azure_key.ps1
echo.
pause
