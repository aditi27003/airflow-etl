@echo off
cd /d "%~dp0"
set PY=
for %%P in ("%USERPROFILE%\anaconda3\python.exe" "%USERPROFILE%\anaconda4\python.exe" "%LOCALAPPDATA%\anaconda3\python.exe" "C:\ProgramData\anaconda3\python.exe") do if exist %%P if not defined PY set PY=%%P
if not defined PY (where py >nul 2>&1 && set PY=py)
if not defined PY (echo Python not found. Install Python 3 or Anaconda. & pause & exit /b 1)
echo Using %PY%
%PY% -m pip install --quiet fastapi uvicorn pandas
echo Starting FastAPI on http://localhost:8000  (keep this window open)
%PY% -m uvicorn main:app --host 0.0.0.0 --port 8000
pause
