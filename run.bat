@echo off
setlocal

git submodule update --init --recursive

IF NOT EXIST "tools\data\SD-Tag-Editor\.installed" (
    echo 🛠 SD-Tag-Editor needs to be installed.
    echo 💡 Launching installer...
    call tools\data\SD-Tag-Editor\install.bat

    set /p CONFIRM=✅ Did the install complete successfully? (y/N):
    if /I NOT "%CONFIRM%"=="Y" if /I NOT "%CONFIRM%"=="y" (
        echo ❌ Installation not confirmed. Aborting.
        exit /b 1
    )
    type nul > tools\data\SD-Tag-Editor\.installed
)

IF NOT EXIST "tools\data\venv\" (
    echo ⚙️ Creating virtual environment...
    python -m venv tools\data\venv
)

call tools\data\venv\Scripts\activate.bat
pip install --upgrade pip
pip install -r tools\requirements.txt

python tools\gui.py
endlocal
