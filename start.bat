@echo off
cd /d %~dp0
setlocal

set "PY_CMD="
where python >nul 2>nul
if %errorlevel%==0 set "PY_CMD=python"
if not defined PY_CMD (
  where py >nul 2>nul
  if %errorlevel%==0 set "PY_CMD=py"
)

if not defined PY_CMD (
  echo Python is not installed or not in PATH.
  echo Install Python 3.10+ and retry.
  pause
  exit /b 1
)

if exist requirements.txt (
  echo Installing dependencies from requirements.txt...
  %PY_CMD% -m pip install -r requirements.txt
  if errorlevel 1 (
    echo Failed to install requirements.
    pause
    exit /b 1
  )
) else (
  echo Checking Flask...
  %PY_CMD% -c "import flask" >nul 2>nul
  if errorlevel 1 (
    echo Flask is not installed. Installing...
    %PY_CMD% -m pip install Flask
    if errorlevel 1 (
      echo Failed to install Flask. Run manually: %PY_CMD% -m pip install Flask
      pause
      exit /b 1
    )
  )
)

echo Starting NM-Bank...
%PY_CMD% server.py
pause
