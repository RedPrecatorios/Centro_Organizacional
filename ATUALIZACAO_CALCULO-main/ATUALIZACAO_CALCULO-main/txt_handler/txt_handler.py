import os
from colorama import Fore, Style
from datetime import datetime

class TxtHandler:
    def __init__(self) -> None:
        self.path = os.path.join(os.getcwd(), "txt_handler", "txt_files")
        os.makedirs(self.path, exist_ok=True)

    def save_max_id(self, max_id):
        try:
            file_path = os.path.join(self.path, "max_id.txt")
            current_date = datetime.now()

            with open(file_path, "a+") as file:
                file.write(f"\nDATE: {current_date}\n\tMAX ID: {max_id}\n")
                file.close()
        except Exception as e:
            print(f"\n{Fore.RED}[x] ERROR:\n\t{str(e)}{Style.RESET_ALL}")
            
    def save_DICTS(self, json_dict):
        try:
            file_path = os.path.join(self.path, "json_dicts.txt")
            current_date = datetime.now()
            
            with open(file_path, "a+") as file:
                file.write(f"\n{current_date}\n{str(json_dict)}\n")
                file.close()
        except Exception as e:
            print(f"\n{Fore.RED}[x] ERROR:\n\t{str(e)}{Style.RESET_ALL}")
            
            
    def save_ERRORs(self, main_dict):
        try:
            file_path = os.path.join(self.path, "save_ERRORs.txt")
            
            with open(file_path, "a+") as file:
                file.write(str(main_dict)+'\n')
                
        except Exception as e:
            print(f"\n{Fore.RED}[x] ERROR:\n\t{str(e)}{Style.RESET_ALL}")
            
    def save_last_checked_id(self, last_checked_id):
        try:
            file_path = os.path.join(self.path, "last_checked_id.txt")
            
            with open(file_path, "w") as file:
                file.write(last_checked_id)
                file.close()
                
        except Exception as e:
            print(f"\n{Fore.RED}[x] ERROR:\n\t{str(e)}{Style.RESET_ALL}")
            
    def read_last_checked_id(self):
        try:
            file_path = os.path.join(self.path, "last_checked_id.txt")
            
            with open(file_path, 'r') as file:
                last_checked_id = int(file.read())
            return last_checked_id
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"\n{Fore.RED}[x] ERROR:\n\t{str(e)}{Style.RESET_ALL}")