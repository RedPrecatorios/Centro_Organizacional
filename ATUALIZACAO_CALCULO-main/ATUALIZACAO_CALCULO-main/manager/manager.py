import os
import re
import json
import glob
import shutil
import PyPDF2
import schedule
import requests
import zipfile
import traceback
import pandas as pd
import pymysql

from datetime import datetime
from colorama import Fore, Style

# Google Drive: upload pós-planilha em ``calculation_automation.py`` + ``google_api/drive.py``
# (GOOGLE_DRIVE_* no .env); a classe ``Drive`` mantém-se para compatibilidade.
from general_functions.general_functions import *
from db_handler.db_handler import DBHandler
from txt_handler.txt_handler import TxtHandler
from calculation_automation.calculation_automation import CalculationAutomation


def _coleta_mysql_config():
    """Segundo banco (coleta_depre). Sem COLETA_MYSQL_HOST = não liga (result = None)."""
    h = (os.getenv("COLETA_MYSQL_HOST") or "").strip()
    if not h:
        return None
    try:
        return {
            "host": h,
            "user": (os.getenv("COLETA_MYSQL_USER") or "acc_ext").strip(),
            "password": os.getenv("COLETA_MYSQL_PASSWORD", "") or "",
            "database": (os.getenv("COLETA_MYSQL_DATABASE") or "coleta_depre").strip(),
            "port": int((os.getenv("COLETA_MYSQL_PORT") or "3306").strip()),
        }
    except ValueError:
        return None


class Manager:
    def __init__(self, today) -> None:
        self.pattern = r"(JAN|FEV|MAR|ABR|MAI|JUN|JUL|AGO|SET|OUT|NOV|DEZ)\s+(.*?)$"
        self.txt_handler = TxtHandler()
        self.today = today
        # self.verify_indice()

    @staticmethod
    def _db_ok(dh) -> bool:
        return dh is not None and getattr(dh, "ok", False)

    def start_conn(self):
        try:
            db_handler = DBHandler()
            if not db_handler.ok:
                return None
            return db_handler
        except Exception as e:
            print(f"\n[x] ERROR:\n\t{e}")
            return None

    def get_max_id(self):
        try:
            db_handler = self.start_conn()
            if not self._db_ok(db_handler):
                return None
            max_id = db_handler.select_query(
                columns="id", table="precainfosnew", args="ORDER BY id DESC LIMIT 1"
            )
            current_date = datetime.now()
            print(f"\n\tMAX ID: {max_id} | {current_date}\n")
            self.txt_handler.save_max_id(max_id)
            db_handler.end_conn()
            return max_id
        except Exception as e:
            print(f"\n[x] ERROR:\n\t{e}")
            return None

    def clean_temp(self):
        _tmp = os.environ.get("TMPDIR") or os.environ.get("TEMP") or os.environ.get("TMP")
        if not _tmp:
            import tempfile
            _tmp = tempfile.gettempdir()
        temp_folder_path = os.path.join(_tmp, "*")
        temp_files = glob.glob(temp_folder_path)
        for file in temp_files:
            try:
                if os.path.isfile(file):
                    os.remove(file)
                elif os.path.isdir(file):
                    shutil.rmtree(file)
            except Exception as e:
                print(f"\nError deleting file: {file}\n\t{str(e)}")

    def clean_numeric_value(self, value):
        """
        Remove caracteres não numéricos, exceto ponto e vírgula. Converte para float, tratando vírgula como separador decimal.
        """
        import re

        if pd.isna(value):
            return 0.0
        cleaned = re.sub(r"[^\d.,-]", "", str(value))
        cleaned = cleaned.replace(",", ".")
        if cleaned.count(".") > 1:
            parts = cleaned.split(".")
            cleaned = "".join(parts[:-1]) + "." + parts[-1]
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def run_atualizacao_calculo(self, prec_id: int) -> dict:
        """
        Actualização de cálculo para **um** ``id`` de ``precainfosnew`` (o mesmo
        que ``id_precainfosnew`` na tabela ``memoria_calculo``) — p.ex. API Flask
        a partir do botão «Atualizar Cálculo».
        """
        from manager.run_single_calculo import execute_atualizacao_calculo

        return execute_atualizacao_calculo(self, prec_id)

    def run(self):
        try:
            max_id = self.txt_handler.read_last_checked_id()

            if not max_id or max_id == 0:
                max_id = self.get_max_id()

            first_execution = True

            while True:

                self.clean_temp()
                if not first_execution:
                    max_id = self.txt_handler.read_last_checked_id()
                    current_date = datetime.now()
                    print(f"\n\tMAX ID: {max_id} | {current_date}\n")
                    self.txt_handler.save_max_id(max_id)
                    if max_id == 0:
                        max_id = self.get_max_id()

                # query = (
                #     f"SELECT id, Depre FROM precainfosnew  WHERE  Data_Decisao is not NULL "
                #     f"AND (Calculo_Atualizado = '0.00' OR Calculo_Atualizado IS NULL) ORDER BY id DESC;"
                # )  # AND (UPDATES_INDEX < 1 OR UPDATES_INDEX IS NULL)
                # query = f"""SELECT id, Depre FROM precainfosnew WHERE id in (7472);"""  # 1192059 | 1196689

                query = f"""SELECT id, Depre FROM precainfosnew WHERE id in (1346953);"""

                
                ERRORs_path = os.path.join(
                    os.path.abspath(os.path.dirname(__file__)),
                    "..",
                    "calculation_automation",
                    "ERRORs",
                )

                invalid_ids = []

                if os.path.exists(ERRORs_path):
                    pattern_id = r"ID:\s*(\d*)"
                    error_file = "ERRORs.txt"
                    with open(
                        os.path.join(ERRORs_path, error_file), "r", encoding="utf-8"
                    ) as file:
                        lines = file.readlines()
                        for line in lines:
                            match = re.search(pattern_id, line)
                            if match:
                                invalid_ids.append(int(match.group(1)))

                db_handler = self.start_conn()
                if not self._db_ok(db_handler):
                    print(
                        f"\n{Fore.LIGHTRED_EX}[x] MySQL indisponível. Ajuste o .env (HOST, DB, DB_USER, PASS). "
                        f"Aguardando 5 min…{Style.RESET_ALL}\n"
                    )
                    cronometer(timeout=300)
                    continue

                db_handler.cursor.execute(query)
                new_records_ids = list(db_handler.cursor.fetchall())

                db_handler.config.close()
                db_handler.cursor.close()
                db_handler = None

                if new_records_ids:
                    print(f"\n\t{Fore.CYAN}[+] New Records found\n{Style.RESET_ALL}")
                    last_checked_id = 0

                    for id, depre in new_records_ids:
                        verificar_meses = False

                        self.clean_temp()
                        id = int(id)
                        print(f"\n\tID: [{id}]\n")

                        db_handler = self.start_conn()
                        if not self._db_ok(db_handler):
                            print(
                                f"\n{Fore.LIGHTRED_EX}[x] MySQL indisponível, a saltar id={id}{Style.RESET_ALL}"
                            )
                            cronometer(timeout=60)
                            continue
                        last_checked_id = str(id)
                        query = f"SELECT * FROM precainfosnew WHERE id = {id};"
                        db_handler.cursor.execute(query)
                        infos = list(db_handler.cursor.fetchall())

                        entidade_devedora = str(infos[0][24]).strip().replace("  ", " ")

                        requerente = str(infos[0][29]).strip().replace("  ", " ")
                        oc = str(infos[0][3]).strip()
                        spprev = str(infos[0][6]).strip()
                        iamspe = str(infos[0][7]).strip()
                        ipesp = str(infos[0][8]).strip()
                        assit_med_hopital = str(infos[0][9]).strip()
                        inst_prev_caixa_benef = str(infos[0][10]).strip()
                        assist_med_caixa_benef = str(infos[0][11]).strip()
                        processo_principal = str(infos[0][32]).strip()
                        numero_de_processo = str(infos[0][12]).strip()
                        numero_de_incidente = str(infos[0][13]).strip()
                        data_base = str(infos[0][18]).strip()
                        data_decisao = str(infos[0][19]).strip()
                        principal_liquido = str(infos[0][20]).strip()
                        juros_moratorio = str(infos[0][21]).strip()
                        juros_compensatorio = str(infos[0][22]).strip()
                        ep = str(infos[0][35]).strip()
                        inst_prev = str(infos[0][46]).strip()
                        numero_de_meses = int(str(infos[0][50]).strip())
                        try:
                            numero_de_meses_termo = int(str(infos[0][51]).strip())
                        except (IndexError, ValueError):
                            numero_de_meses_termo = 0

                        if numero_de_meses > 0:
                            meses_validados = numero_de_meses
                        elif numero_de_meses_termo > 0:
                            meses_validados = numero_de_meses_termo
                        else:
                            meses_validados = 0

                        # print(f"infos: {infos}")
                        # print("\n\n")
                        # --- consulta coleta_depre (opcional) ---
                        result = None
                        cfg = _coleta_mysql_config()
                        if cfg and depre:
                            try:
                                ext_conn = pymysql.connect(
                                    host=cfg["host"],
                                    user=cfg["user"],
                                    password=cfg["password"],
                                    database=cfg["database"],
                                    port=cfg["port"],
                                )
                                ext_cursor = ext_conn.cursor()
                                ext_cursor.execute(
                                    "SELECT valor_pago, saldo FROM coleta_depre WHERE processo_depre = %s LIMIT 1",
                                    (depre,),
                                )
                                result = ext_cursor.fetchone()
                                print(f"{id} : {depre}")
                                if result:
                                    print(
                                        f"Registro encontrado no segundo banco para {id} : {depre}"
                                    )
                                else:
                                    print(
                                        f"NÃO encontrado no segundo banco para {id} : {depre}"
                                    )
                                ext_cursor.close()
                                ext_conn.close()
                            except Exception as e:
                                print(
                                    f"Erro ao conectar ou consultar o segundo banco: {e}"
                                )
                                result = None
                        if result:
                            valor_pago, saldo = result
                            try:
                                valor_pago = float(valor_pago)
                            except Exception:
                                valor_pago = 0.0
                            try:
                                saldo = float(saldo)
                            except Exception:
                                saldo = 0.0
                        else:
                            valor_pago = None
                            saldo = None

                        update_query = None
                        print(f"Numero de meses: {meses_validados}")
                        print(f"Saldo: {saldo}")
                        print(f"Valor_Pago: {valor_pago}")

                        if saldo is None or valor_pago is None:
                            if meses_validados == 0 and "INSS" not in entidade_devedora:
                                verificar_meses = True
                        elif saldo == 0:
                            update_query = f"UPDATE precainfosnew SET Calculo_Atualizado = 'Sem saldo' WHERE id = {id};"
                        elif valor_pago > 0:
                            update_query = f"UPDATE precainfosnew SET Calculo_Atualizado = 'Prioridade' WHERE id = {id};"
                        elif valor_pago == 0 and meses_validados == 0:
                            verificar_meses = True
                        elif valor_pago == 0 and meses_validados > 0:
                            pass
                        else:
                            print("não se encaixou em nenhuma regra")

                        if update_query:
                            try:
                                db_handler2 = self.start_conn()
                                if not self._db_ok(db_handler2):
                                    print("MySQL indisponível, não foi possível aplicar update_query")
                                    continue
                                db_handler2.cursor.execute(update_query)
                                db_handler2.config.commit()
                                db_handler2.config.close()
                                db_handler2.cursor.close()
                                self.txt_handler.save_last_checked_id(last_checked_id)
                                continue
                            except Exception as e:
                                print(f"Erro ao atualizar o banco principal: {e}")
                                continue

                        if verificar_meses and "INSS" not in entidade_devedora:
                            try:
                                db_handler2 = self.start_conn()
                                if not self._db_ok(db_handler2):
                                    print("MySQL indisponível, não foi possível marcar Verificar Meses")
                                else:
                                    db_handler2.cursor.execute(
                                        f"UPDATE precainfosnew SET Calculo_Atualizado = 'Verificar Meses' WHERE id = {id};"
                                    )
                                    db_handler2.config.commit()
                                    db_handler2.config.close()
                                    db_handler2.cursor.close()
                                self.txt_handler.save_last_checked_id(last_checked_id)
                            except Exception as e:
                                print(f"Erro ao atualizar o banco principal: {e}")

                        main_dict = {
                            "Requerente": "",
                            "Entidade_Devedora": "",
                            "Processo": "",
                            "Oc": "",
                            "EP": "",
                            "Cumprimento": "",
                            "Incidente": "",
                            "Data_Base": "",
                            "Data_Inscrição": "",
                            "Principal_Liquido": "",
                            "Juros_Moratorio": "",
                            "Despesas": "",
                            "IPESP/IAMSP": "",
                            "SPPREV": "",
                            "IAMSPE": "",
                            "IPESP": "",
                            "ASSIT_MED_HOSPITAL": "",
                            "INST_PREV_CAIXA_BENEF": "",
                            "ASSIST_MED_CAIXA_BENEF": "",
                            "INST_PREV": "",
                            "Numero_de_Meses": meses_validados,
                            "id": id,
                        }

                        try:
                            main_dict["SPPREV"] = float(format_dict_values(spprev))
                            main_dict["IAMSPE"] = float(format_dict_values(iamspe))
                            main_dict["IPESP"] = float(format_dict_values(ipesp))
                            main_dict["ASSIT_MED_HOSPITAL"] = float(
                                format_dict_values(assit_med_hopital)
                            )
                            main_dict["INST_PREV_CAIXA_BENEF"] = float(
                                format_dict_values(inst_prev_caixa_benef)
                            )
                            main_dict["ASSIST_MED_CAIXA_BENEF"] = float(
                                format_dict_values(assist_med_caixa_benef)
                            )
                            main_dict["INST_PREV"] = float(format_dict_values(inst_prev))
                        except Exception as e:
                            print(e)

                        if main_dict["ASSIT_MED_HOSPITAL"] == main_dict["ASSIST_MED_CAIXA_BENEF"]:
                            main_dict["ASSIST_MED_CAIXA_BENEF"] = 0

                        if main_dict["INST_PREV"] == 0 or (
                            main_dict["INST_PREV"]
                            not in [
                                main_dict["SPPREV"],
                                main_dict["IAMSPE"],
                                main_dict["IPESP"],
                                main_dict["ASSIT_MED_HOSPITAL"],
                                main_dict["INST_PREV_CAIXA_BENEF"],
                                main_dict["ASSIST_MED_CAIXA_BENEF"],
                            ]
                        ):
                            main_dict["IPESP/IAMSP"] = (
                                0
                                + main_dict["ASSIT_MED_HOSPITAL"]
                                + main_dict["INST_PREV_CAIXA_BENEF"]
                                + main_dict["ASSIST_MED_CAIXA_BENEF"]
                            )
                        else:
                            main_dict["IPESP/IAMSP"] = (
                                main_dict["SPPREV"]
                                + main_dict["IAMSPE"]
                                + main_dict["IPESP"]
                                + main_dict["ASSIT_MED_HOSPITAL"]
                                + main_dict["INST_PREV_CAIXA_BENEF"]
                                + main_dict["ASSIST_MED_CAIXA_BENEF"]
                            )

                        main_dict["Requerente"] = requerente
                        main_dict["Entidade_Devedora"] = entidade_devedora
                        main_dict["Processo"] = numero_de_processo
                        main_dict["Oc"] = oc
                        main_dict["EP"] = ep
                        main_dict["Cumprimento"] = processo_principal
                        main_dict["Incidente"] = numero_de_incidente
                        main_dict["Data_Base"] = data_base
                        main_dict["Data_Inscrição"] = data_decisao
                        main_dict["Principal_Liquido"] = float(
                            format_dict_values(principal_liquido)
                        )
                        main_dict["Juros_Moratorio"] = self.clean_numeric_value(
                            format_dict_values(juros_moratorio)
                        )
                        main_dict["Despesas"] = 0

                        json_dict = json.dumps(main_dict, indent=4)
                        self.txt_handler.save_DICTS(json_dict)

                        db_handler.config.close()
                        db_handler.cursor.close()
                        db_handler = None

                        calculation_automation = CalculationAutomation(
                            main_dict, self.today
                        )
                        self.today = calculation_automation.check_day()
                        calculation_automation.edit_cells()

                        if not verificar_meses:
                            calculation_automation.get_calculo_atualizado(id, db_handler)
                            self.txt_handler.save_last_checked_id(last_checked_id)
                        else:
                            print(
                                f"[+] {main_dict['Processo']} - {main_dict['Incidente']}: Verificar meses"
                            )

                    first_execution = False
                    exit()
                    continue

                else:
                    print(
                        f"\n\t{Fore.LIGHTYELLOW_EX}[-] New Records NOT found{Style.RESET_ALL}"
                    )
                    cronometer(timeout=300)
                    continue

        except Exception as e:
            print(f"\n[x] ERROR:\n\t{e}")
            print(traceback.print_exc())
