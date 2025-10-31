import os
import platform
import subprocess
import sys
from pathlib import Path

def detect_os():
    # Detect the platform (Windows or Linux)
    if os.name == "nt":  # For Windows
        return 'nt'
    elif os.name == "posix":  # For Linux (and other Unix-like systems)
        system_name = platform.system().lower()
        if system_name == "linux":
            return 'linux'
    else:
        print("⚠️ Unsupported OS. This function only works on Windows and Linux.")
        sys.exit(1)

def create_venv_and_install():
    system = detect_os()
    script_dir = Path(__file__).parent.resolve()
    venv_dir = script_dir / 'venv'
    venv_python = venv_dir / ('Scripts' if system == 'nt' else 'bin') / 'python'
    pip_executable = venv_dir / ('Scripts' if os.name == 'nt' else 'bin') / 'pip'
    requirements_file = script_dir / '..' / 'requirements.txt'

    # Step 1: Create venv
    try:
        subprocess.check_call([sys.executable, "-m", "venv", str(venv_dir)])
        print(f"✅ Virtual environment created at {venv_dir}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to create virtual environment: {e}")
        sys.exit(1)

    # Step 3: Install requirements
    print("🧠 Installing requirements.")
    subprocess.check_call([str(pip_executable), 'install', '-r', str(requirements_file), ])

# Call the function to create the venv and install requirements
if __name__ == "__main__":
    try:
        create_venv_and_install()
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        sys.exit(1)
