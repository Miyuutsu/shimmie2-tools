@echo off
cd /d "%~dp0"

set REPO_URL=https://github.com/Miyuutsu/shimmie2-tools.git

if not exist ".git" (
  dir /b 2>nul | findstr . >nul
  if not errorlevel 1 (
    echo ❌ Error: This directory is not empty and has no Git repo.
    echo    Refusing to initialize to avoid overwriting your files.
    pause
    exit /b 1
  )

  echo 🔍 No .git directory found. Initializing Git...
  git init
  git remote add origin %REPO_URL%
  echo run.bat>> .git\info\exclude
  echo run.sh>> .git\info\exclude
  git fetch origin
  git reset --hard origin/master
  echo ✅ Repository initialized from %REPO_URL%
)

if not exist "tools\data\SD-Tag-Editor\run.bat" (
  echo 📦 Initializing submodules...
  git submodule update --init --recursive
)

if not exist "tools\data\SD-Tag-Editor\.installed" (
  echo ⚙️ Installing SD-Tag-Editor...
  call tools\data\SD-Tag-Editor\run.bat
)

echo 🚀 Launching Shimmie Tools GUI...
python tools\gui.py
pause
