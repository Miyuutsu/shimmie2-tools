#!/bin/bash

cd "$(dirname "$0")"

REPO_URL="https://github.com/Miyuutsu/shimmie2-tools.git"

# Check if directory is empty
if [ ! -d ".git" ]; then
  if [ "$(ls -A)" ]; then
    echo "❌ Error: This directory is not empty and has no Git repo."
    echo "   Refusing to initialize to avoid overwriting your files."
    exit 1
  fi

  echo "🔍 No .git directory found. Initializing Git..."
  git init
  # Exclude bootstrap scripts from Git tracking
  git remote add origin "$REPO_URL"
  echo "run.sh" >> .git/info/exclude
  echo "run.bat" >> .git/info/exclude
  git fetch origin
  git reset --hard origin/master
  echo "✅ Repository initialized from $REPO_URL"
fi

# Initialize submodules if needed
if [ ! -f "tools/data/SD-Tag-Editor/run.sh" ]; then
  echo "📦 Initializing submodules..."
  git submodule update --init --recursive
fi

# Run SD-Tag-Editor install if needed
if [ ! -f "tools/data/SD-Tag-Editor/.installed" ]; then
  echo "⚙️ Installing SD-Tag-Editor..."
  bash tools/data/SD-Tag-Editor/run.sh
fi

echo "🚀 Launching Shimmie Tools GUI..."
python3 tools/gui.py
