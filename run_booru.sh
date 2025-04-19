#!/bin/bash

if [ -d "venv" ]; then
    source venv/bin/activate
else
    chmod +x install.sh
    ./install.sh no_pause
    source venv/bin/activate
fi
python run_booru.py --batch_size=20 --subfolder=True --shimmie=True --no-prune --threads=16
read -p "Press any key to continue..."
deactivate
