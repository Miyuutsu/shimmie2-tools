@echo off
setlocal

:: Move to script directory
cd /d %~dp0

set "REPO_URL=https://github.com/Miyuutsu/shimmie2-tools.git"
set "VENV_DIR=tools\data\venv"

:: Check if python3.11 is available
where python3.11 >nul 2>&1
if errorlevel 1 (
    echo ❌ Python 3.11 not found. Please install Python 3.11 and try again.
    pause
    exit /b 1
)

:: Git setup
if not exist ".git" (
    for /f %%i in ('dir /b') do (
        echo ❌ Error: This directory is not empty and has no Git repo.
        echo    Refusing to initialize to avoid overwriting your files.
        pause
        exit /b 1
    )
    echo 🔍 No .git directory found. Initializing Git...
    git init
    git remote add origin %REPO_URL%
    echo run.sh>>.git\info\exclude
    echo run.bat>>.git\info\exclude
    git fetch origin
    git reset --hard origin/master
    echo ✅ Repository initialized from %REPO_URL%
)

:: Initialize submodules if needed
if not exist "tools\SD-Tag-Editor\run.bat" (
    echo 📦 Initializing submodules...
    git submodule update --init --recursive
)

:: Install SD-Tag-Editor if needed
if not exist "tools\SD-Tag-Editor\.installed" (
    echo ⚙️ Installing SD-Tag-Editor...
    call tools\SD-Tag-Editor\install.bat
)

:: Create venv using Python 3.11
if not exist "%VENV_DIR%" (
    echo 🐍 Creating virtual environment with Python 3.11...
    python3.11 -m venv "%VENV_DIR%"
    echo 📦 Installing requirements from tools/requirements.txt...
    call "%VENV_DIR%\Scripts\activate.bat"
    python -m pip install --upgrade pip
    python -m pip install -r tools\requirements.txt
)

:: Activate and run GUI
call "%VENV_DIR%\Scripts\activate.bat"
echo 🚀 Launching Shimmie Tools GUI...
python tools\gui.py

endlocal
