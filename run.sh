#!/bin/bash

cd "$(dirname "$0")"

REPO_URL="https://github.com/Miyuutsu/shimmie2-tools.git"
PYTHON=$(command -v python3.11)
safety="$1"

# Ensure python3.11 is available
if [ -z "$PYTHON" ]; then
  echo "❌ Python 3.11 not found. Please install Python 3.11 and try again."
  exit 1
fi

# Check if directory is not a git repo AND not explicitly disabled
if [ ! -d ".git" ]; then
  if [ "$(ls -A | grep -Ev '\.sh$|\.bat$')" ] && [ "$safety" != "off" ]; then
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
if [ ! -f "backend/sd_tag_editor/run.sh" ]; then
  echo "📦 Initializing submodules..."
  git submodule update --init --recursive
fi

# Perform installation
if [ ! -d "backend/sd_tag_editor/venv" ]; then
  echo "Performing installation..."
  $PYTHON backend/scripts/install.py
fi

# Launch GUI
source backend/sd_tag_editor/venv/bin/activate
echo "🚀 Launching Shimmie Tools GUI..."
python backend/gui.py
