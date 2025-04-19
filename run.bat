@echo off
cd /d %~dp0

REM Check if venv exists
if not exist "venv\" (
    echo Creating virtual environment...
    python -m venv venv
)

REM Activate venv
call venv\Scripts\activate.bat

REM Install dependencies
pip install -r tools/requirements.txt

REM Launch the GUI
python tools/gui.py
