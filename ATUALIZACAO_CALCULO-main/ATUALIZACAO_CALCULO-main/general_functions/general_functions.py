import sys
import time

def format_dict_values(value):
    if not value or value == 'Não informado pelo peticionante' or value == "0.00" or value == "" or value == ".":
        value = '0'
    try:
        new_value = value.replace(",", ".")
        new_value = new_value.rsplit('.', 1)
        new_value = new_value[0] + ',' + new_value[1]
        new_value = new_value.replace(".", "").replace(",", ".")
    except IndexError:
        new_value = 0
        
    return new_value

def cronometer(timeout):
    try:
        start_time = time.time()
        print("\n")
        while time.time() - start_time < timeout:
            elapsed_time = int(time.time()) - start_time
            sys.stdout.write("\r")
            sys.stdout.write(f"\t[T] Timeout [{timeout}]: {int(elapsed_time)} sec")
            sys.stdout.flush()
            time.sleep(1)
            
    except Exception as e:
        print(f"\n[x] ERROR:\n\t{e}")