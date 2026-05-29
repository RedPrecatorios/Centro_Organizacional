import os
import sys
import time
import subprocess
from datetime import datetime

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
    """Remove só temporários do fluxo de cálculo (prefixo lo_memoria_), não todo o /tmp."""
    from manager.manager import Manager

    Manager(datetime.now()).clean_temp()

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