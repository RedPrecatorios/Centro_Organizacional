import os
import sys
import time
import glob
import shutil
import tempfile
import subprocess
from datetime import datetime

def _temp_glob_pattern():
    t = os.environ.get("TMPDIR") or os.environ.get("TEMP") or os.environ.get("TMP")
    if not t:
        t = tempfile.gettempdir()
    return os.path.join(t, "*")

def cronometro(timeout):
    start_time = time.time()
    print("\n")
    while time.time() - start_time < timeout:
        elapsed_time = int(time.time()) - start_time
        sys.stdout.write("\r")
        sys.stdout.write(f"\t[T] Timeout [{timeout}]: {int(elapsed_time)} segundos")
        sys.stdout.flush()
        time.sleep(1)
        
def clean_temp():
    temp_folder_path = _temp_glob_pattern()
    temp_files = glob.glob(temp_folder_path)
    
    for file in temp_files:
        try:
            if os.path.isfile(file):
                os.remove(file)
            elif os.path.isdir(file):
                shutil.rmtree(file)
        except Exception as e:
            print(f"\nError deleting file: {file}\n\t{str(e)}")

if __name__ == "__main__":
    script = "main"
    path_to_script = os.path.join(os.getcwd(), "main.py")

    while True:
        print(f"\n\t[-] Clean TEMP files\n")
        clean_temp()
        
        print(f"\n\t[§] Executing script [{script}]:\n")
        response = subprocess.call([sys.executable, path_to_script])

        timeout = 60
        cronometro(timeout)

        print(f"\n\t[#] Restarting...\n")