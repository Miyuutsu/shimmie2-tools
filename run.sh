#!/bin/bash

cd "$(dirname "$0")"

REPO_URL="https://github.com/Miyuutsu/shimmie2-tools.git"
VENV_DIR="tools/data/venv"
PYTHON=$(command -v python3.11)

# Ensure python3.11 is available
if [ -z "$PYTHON" ]; then
  echo "❌ Python 3.11 not found. Please install Python 3.11 and try again."
  exit 1
fi

# Check if directory is empty and not a git repo
if [ ! -d ".git" ]; then
  if [ "$(ls -A)" ]; then
    echo "❌ Error: This directory is not empty and has no Git repo."
    echo "   Refusing to initialize to avoid overwriting your files."
    exit 1
  fi

  echo "🔍 No .git directory found. Initializing Git..."
  git init
  git remote add origin "$REPO_URL"
  echo "run.sh" >> .git/info/exclude
  echo "run.bat" >> .git/info/exclude
  git fetch origin
  git reset --hard origin/master
  echo "✅ Repository initialized from $REPO_URL"
fi

# Initialize submodules
if [ ! -f "tools/SD-Tag-Editor/run.sh" ]; then
  echo "📦 Initializing submodules..."
  git submodule update --init --recursive
fi

# Run SD-Tag-Editor install if needed
if [ ! -f "tools/SD-Tag-Editor/.installed" ]; then
  echo "⚙️ Installing SD-Tag-Editor..."
  bash tools/SD-Tag-Editor/install.sh
fi

# Create venv using Python 3.11 if missing
if [ ! -d "$VENV_DIR" ]; then
  echo "🐍 Creating virtual environment with Python 3.11..."
  "$PYTHON" -m venv "$VENV_DIR"
  # Activate venv and install requirements
  echo "📦 Installing requirements from tools/requirements.txt..."
  source "$VENV_DIR/bin/activate"
  pip install --upgrade pip
  pip install -r tools/requirements.txt
fi

# Launch GUI
echo "🚀 Launching Shimmie Tools GUI..."
python tools/gui.py
