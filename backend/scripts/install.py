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

def detect_nvidia_gpu(system):
    try:
        if system == 'nt':
            # The path to nvidia-smi.exe in Windows, change if necessary
            nvidia_smi_path = Path("C:/Program Files/NVIDIA Corporation/NVSMI/nvidia-smi.exe").resolve()
            result = subprocess.run([nvidia_smi_path, '--query-gpu=name', '--format=csv,noheader'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        elif system == 'linux':
            result = subprocess.run(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)

        # Check if the result is valid and if a GPU was detected
        if result is not None and result.returncode == 0 and result.stdout.strip():
            print(f"🧠 Detected NVIDIA GPU: {result.stdout.strip()}")
            return 'nvidia'
        else:
            print(f"🧠 Detected non-NVIDIA GPU: {result.stdout.strip()}\nFalling back to CPU")
            return 'no'
    except Exception as e:
        print(f"❌ Error detecting GPU: {e}")
        return 'no'

def create_venv_and_install():
    system = detect_os()
    nv = detect_nvidia_gpu(system)
    script_dir = Path(__file__).parent.resolve()
    editor_dir = script_dir / '..' / 'sd_tag_editor'
    venv_dir = editor_dir / 'venv'
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

    # Step 2: Re-run script using the venv's Python if not already
    if Path(sys.prefix) != venv_dir:
        print("🔁 Re-running script inside virtual environment...")
        os.execv(str(venv_python), [str(venv_python), *sys.argv])

    # Step 3: Install requirements
    try:
        if nv.strip().lower() == 'nvidia':
            print("🧠 Using CUDA Optimized Nvidia installation.")
            subprocess.check_call([str(pip_executable), 'install', '-r', str(requirements_file), '--extra-index-url=https://download.pytorch.org/whl/cu121'])
        else:
            print("🧠 Defaulting to CPU-only installation.")
            subprocess.check_call([str(pip_executable), 'install', '-r', str(requirements_file), ])
    except Exception as e:
        print(f"❌ Failed to install requirements. Application may not run properly. Error:\n{e}\n")
        print("🔁 You can try manually installing the requirements if the script failed.")

# Call the function to create the venv and install requirements
if __name__ == "__main__":
    try:
        create_venv_and_install()
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        sys.exit(1)
