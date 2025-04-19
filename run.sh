#!/bin/bash

# Initialize submodules
git submodule update --init --recursive

# Check install marker
if [ ! -f "tools/data/SD-Tag-Editor/.installed" ]; then
    echo "🛠 SD-Tag-Editor needs to be installed."
    echo "💡 Opening its installer..."

    # Move into the SD-Tag-Editor folder before calling run.sh
    pushd tools/data/SD-Tag-Editor > /dev/null
    bash install.sh
    popd > /dev/null

    echo ""
    read -rp "✅ Did the install complete successfully? (y/N): " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        touch tools/data/SD-Tag-Editor/.installed
    else
        echo "❌ Installation not confirmed. Aborting."
        exit 1
    fi
fi


# Create virtualenv
if [ ! -d "tools/data/venv" ]; then
    echo "⚙️ Creating virtual environment..."
    python3 -m venv tools/data/venv
    source tools/data/venv/bin/activate
    pip install --upgrade pip
    pip install -r tools/requirements.txt
fi

source tools/data/venv/bin/activate
python tools/gui.py
