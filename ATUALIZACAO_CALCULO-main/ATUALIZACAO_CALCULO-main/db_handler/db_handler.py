import os
from pathlib import Path

import pymysql
from dotenv import dotenv_values, load_dotenv
from colorama import Fore, Style

# Carrega .env a partir da raiz do projecto (pasta acima de db_handler/), além do cwd.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _mysql_settings():
    """
    Parâmetros de ligação. `dotenv_values` lê o ficheiro .env directamente, para
    a chave USER do ficheiro não ser substituída por USER (utilizador Unix) em
    Linux, típica causa de 'Access denied' para root@localhost (1698 com auth_socket).
    """
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.is_file():
        _parent = _PROJECT_ROOT.parent / ".env"
        if _parent.is_file():
            env_path = _parent
    f: dict = {}
    if env_path.is_file():
        f = {k: v for k, v in (dotenv_values(env_path) or {}).items() if v not in (None, "")}
    if env_path.is_file():
        load_dotenv(env_path)
    load_dotenv()

    def pick(*keys: str) -> str | None:
        for k in keys:
            if k in f and f[k]:
                return f[k]
            v = os.getenv(k)
            if v:
                return v
        return None

    host = pick("HOST", "MYSQL_HOST") or "127.0.0.1"
    database = pick("DB", "MYSQL_DATABASE")
    user = pick("DB_USER", "MYSQL_USER", "USER")
    password = pick("PASS", "MYSQL_PASSWORD", "PASSWORD", "MYSQL_PASS") or ""
    try:
        port = int(pick("PORT", "MYSQL_PORT") or "3306")
    except ValueError:
        port = 3306

    return {
        "host": host,
        "database": database,
        "user": user,
        "password": password,
        "port": port,
    }


class DBHandler:
    def __init__(self) -> None:
        self.config, self.cursor = self.start_conn()

    @property
    def ok(self) -> bool:
        return self.config is not None and self.cursor is not None

    def start_conn(self):
        try:
            s = _mysql_settings()
            if not s.get("user") or not s.get("database"):
                print(
                    f"\n{Fore.RED}[x] .env: defina DB (ou MYSQL_DATABASE) e credenciais "
                    f"(DB_USER, MYSQL_USER ou USER no ficheiro .env).{Style.RESET_ALL}"
                )
                return None, None
            config = pymysql.connect(
                host=s["host"],
                port=s["port"],
                database=s["database"],
                user=s["user"],
                password=s["password"],
                charset="utf8mb4",
            )
            cursor = config.cursor()

            conn_infos = (
                f"\n\t[+] Connection informations:\n"
                f"\t\tLocal running: {config.get_server_info()}\n"
                f"\t\tProtocol running: {config.get_proto_info()}\n"
                f"\t\tHost running: {config.get_host_info()}\n"
            )
            print(
                f"{Fore.LIGHTBLACK_EX}\n\t[---------- Connection Estabilished ----------]: {conn_infos}{Style.RESET_ALL}"
            )
            return config, cursor
        except Exception as error:
            print(f"\n{Fore.RED}[x] ERROR:\n\t{str(error)}{Style.RESET_ALL}")
            if "1698" in str(error) or "Access denied" in str(error):
                print(
                    f"{Fore.LIGHTYELLOW_EX}\tDica: em Ubuntu, o MySQL muitas vezes deixa o "
                    f"user `root` só com autenticação por socket. Crie um utilizador MySQL "
                    f"com palavra-passe (GRANT) e defina DB_USER / PASS no .env.{Style.RESET_ALL}"
                )
            return None, None

    def end_conn(self):
        try:
            if self.config:
                self.cursor.close()
                self.config.close()
        except Exception as e:
            print(f"\n{Fore.RED}[x] ERROR:\n\t{str(e)}{Style.RESET_ALL}")
            
    def check_if_if_has_already_been_updated(self, ID):
        if not self.ok:
            return None
        query = f"SELECT Calculo_Atualizado, UPDATES_INDEX from precainfosnew WHERE id = {ID};"
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        for i in result:
            calculo_atualizado, updates_index = i
            
            if calculo_atualizado:
                if calculo_atualizado == "0.00" or "informado" in str(calculo_atualizado).lower() or updates_index == 0:
                    return True
                else:
                    return False
            else:
                return True
        
    def select_query(self, columns, table, args=None):
        if not self.ok:
            return None
        try:
            if args is not None:
                # columns="MAX(id)", table="precainfosnew", args=None
                query = f"SELECT {columns} FROM {table} {args};"
            else:
                query = f"SELECT {columns} FROM {table};"

            print(f"\n[-] {query}\n")

            self.cursor.execute(query)

            if int(self.cursor.rowcount) > 0:
                if self.cursor.rowcount > 1:
                    result = self.cursor.fetchall()
                else:
                    result = self.cursor.fetchone()[0]
            else:
                result = None

            return result

        except pymysql.err.MySQLError as error:
            print(f"\n{Fore.RED}[x] MYSQL ERROR:\n\t{str(error)}{Style.RESET_ALL}")
            return None

        except Exception as e:
            print(f"\n{Fore.RED}[x] ERROR:\n\t{str(e)}{Style.RESET_ALL}")
            return None