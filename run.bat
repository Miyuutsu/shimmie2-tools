@echo off
setlocal ENABLEEXTENSIONS

REM Change to script directory
cd /d "%~dp0"

REM Set repository URL
set REPO_URL=https://github.com/Miyuutsu/shimmie2-tools.git

REM Try to locate python 3.11
for /f "delims=" %%i in ('where python3.11 2^>nul') do set PYTHON=%%i

set "safety=%~1"

REM Ensure Python 3.11 is available
if not defined PYTHON (
    echo ❌ Python 3.11 not found. Please install Python 3.11 and try again.
    exit /b 1
)

REM Check if directory is not a Git repo AND not explicitly disabled
if not exist ".git" (
    for /f %%f in ('dir /b /a-d ^| findstr /vile ".bat" ".sh"') do (
        if /i not "%safety%"=="off" (
            echo ❌ Error: This directory is not empty and has no Git repo.
            echo    Refusing to initialize to avoid overwriting your files.
            exit /b 1
        )
    )

    echo 🔍 No .git directory found. Initializing Git...
    git init
    git remote add origin "%REPO_URL%"
    echo run.sh>>.git\info\exclude
    echo run.bat>>.git\info\exclude
    git fetch origin
    git reset --hard origin/master
    echo ✅ Repository initialized from %REPO_URL%
)

REM Initialize submodules
if not exist "backend\sd_tag_editor\run.sh" (
    echo 📦 Initializing submodules...
    git submodule update --init --recursive
)

REM Perform installation if venv doesn't exist
if not exist "backend\sd_tag_editor\venv" (
    echo Performing installation...
    "%PYTHON%" backend\scripts\install.py
    type nul > .installed
)

REM Activate virtual environment and launch GUI
call backend\sd_tag_editor\venv\Scripts\activate.bat
echo 🚀 Launching Shimmie Tools GUI...
python backend\gui.py
