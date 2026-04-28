# -*- coding: utf-8 -*-
"""
Uma passagem de actualização ( ``precainfosnew`` + planilha + ``memoria_calculo`` ) por
`id` — mesma regra que o ``for`` em ``Manager.run()`` (acesso a partir do botão na UI).
"""
from __future__ import annotations

import json
import traceback
from typing import Any

import pymysql
from colorama import Fore, Style

from calculation_automation.calculation_automation import CalculationAutomation
from general_functions.general_functions import format_dict_values


def execute_atualizacao_calculo(mgr, prec_id: int) -> dict[str, Any]:
    """
    Executa a actualização de cálculo para **um** ``id`` de ``precainfosnew`` (o mesmo que
    ``id_precainfosnew`` em ``memoria_calculo``).

    Returns
    -------
    dict
        ``ok`` (bool), e ``message`` ou ``error`` (str).
    """
    try:
        rec_id = int(prec_id)
    except (TypeError, ValueError):
        return {"ok": False, "error": "id_precainfosnew inválido."}

    try:
        mgr.clean_temp()
        print(f"\n\t{Fore.CYAN}[atualizacao] id precainfosnew: [{rec_id}]{Style.RESET_ALL}\n")

        db_handler = mgr.start_conn()
        if not mgr._db_ok(db_handler):
            return {
                "ok": False,
                "error": "MySQL indisponível. Verifique o .env (HOST, DB, DB_USER, PASS).",
            }

        try:
            db_handler.cursor.execute(
                "SELECT id, Depre FROM precainfosnew WHERE id = %s",
                (rec_id,),
            )
            id_depre = db_handler.cursor.fetchone()
            if not id_depre:
                try:
                    db_handler.config.close()
                    db_handler.cursor.close()
                except Exception:
                    pass
                return {
                    "ok": False,
                    "error": f"Não existe registo com id = {rec_id} em precainfosnew.",
                }
            _pid, depre = id_depre[0], id_depre[1]
            depre = (depre if depre is not None else "") or ""

            db_handler.cursor.execute(
                "SELECT * FROM precainfosnew WHERE id = %s",
                (rec_id,),
            )
            infos = list(db_handler.cursor.fetchall())
        except Exception as e:
            if db_handler and mgr._db_ok(db_handler):
                try:
                    db_handler.config.close()
                    db_handler.cursor.close()
                except Exception:
                    pass
            return {"ok": False, "error": f"Erro ao consultar precainfosnew: {e}"}

        if not infos:
            try:
                db_handler.config.close()
                db_handler.cursor.close()
            except Exception:
                pass
            return {"ok": False, "error": "SELECT * retornou vazio."}

        verificar_meses = False
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

        result = None
        from manager import manager as mgr_mod

        cfg = mgr_mod._coleta_mysql_config()
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
                ext_cursor.close()
                ext_conn.close()
            except Exception as e:
                print(f"Erro coleta_depre: {e}")
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
            update_query = (
                f"UPDATE precainfosnew SET Calculo_Atualizado = 'Sem saldo' "
                f"WHERE id = {rec_id};"
            )
        elif valor_pago > 0:
            update_query = (
                f"UPDATE precainfosnew SET Calculo_Atualizado = 'Prioridade' "
                f"WHERE id = {rec_id};"
            )
        elif valor_pago == 0 and meses_validados == 0:
            verificar_meses = True
        elif valor_pago == 0 and meses_validados > 0:
            pass
        else:
            print("não se encaixou em nenhuma regra")

        if update_query:
            try:
                if mgr._db_ok(db_handler):
                    try:
                        db_handler.config.close()
                        db_handler.cursor.close()
                    except Exception:
                        pass
                db_handler = None
                db_handler2 = mgr.start_conn()
                if not mgr._db_ok(db_handler2):
                    return {"ok": False, "error": "MySQL indisponível na actualização de estado."}
                db_handler2.cursor.execute(update_query)
                db_handler2.config.commit()
                db_handler2.config.close()
                db_handler2.cursor.close()
                msg = "Sem saldo" if "Sem saldo" in update_query else "Prioridade"
                mgr.txt_handler.save_last_checked_id(str(rec_id))
                return {"ok": True, "message": f"Registo actualizado: {msg} (sem gerar planilha)."}
            except Exception as e:
                return {"ok": False, "error": f"Erro ao actualizar o banco: {e}"}

        if verificar_meses and "INSS" not in entidade_devedora:
            try:
                db_handler2 = mgr.start_conn()
                if not mgr._db_ok(db_handler2):
                    print("MySQL indisponível, não marcou Verificar Meses")
                else:
                    db_handler2.cursor.execute(
                        f"UPDATE precainfosnew SET Calculo_Atualizado = 'Verificar Meses' "
                        f"WHERE id = {rec_id};"
                    )
                    db_handler2.config.commit()
                    db_handler2.config.close()
                    db_handler2.cursor.close()
            except Exception as e:
                print(f"Erro Verificar Meses: {e}")

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
            "id": rec_id,
        }

        try:
            main_dict["SPPREV"] = float(format_dict_values(spprev))
            main_dict["IAMSPE"] = float(format_dict_values(iamspe))
            main_dict["IPESP"] = float(format_dict_values(ipesp))
            main_dict["ASSIT_MED_HOSPITAL"] = float(format_dict_values(assit_med_hopital))
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
        main_dict["Principal_Liquido"] = float(format_dict_values(principal_liquido))
        main_dict["Juros_Moratorio"] = mgr.clean_numeric_value(
            format_dict_values(juros_moratorio)
        )
        main_dict["Despesas"] = 0

        json_dict = json.dumps(main_dict, indent=4)
        mgr.txt_handler.save_DICTS(json_dict)

        try:
            db_handler.config.close()
            db_handler.cursor.close()
        except Exception:
            pass

        calculation_automation = CalculationAutomation(main_dict, mgr.today)
        mgr.today = calculation_automation.check_day()
        calculation_automation.edit_cells()
        drive_link = getattr(calculation_automation, "google_drive_link", None)

        if not verificar_meses:
            calculation_automation.get_calculo_atualizado(rec_id, None)
            mgr.txt_handler.save_last_checked_id(str(rec_id))
            return {
                "ok": True,
                "message": "Cálculo actualizado. Planilha gerada, precainfosnew e memória de cálculo actualizados.",
                "google_drive": {"uploaded": bool(drive_link), "link": drive_link},
            }
        print(
            f"[+] {main_dict['Processo']} - {main_dict['Incidente']}: "
            f"Verificar meses (planilha gerada; actualize o estado manualmente se necessário)"
        )
        return {
            "ok": True,
            "message": "Planilha gerada. Caso ‘Verificar meses’: “Calculo_Atualizado” pode não ter sido preenchido automaticamente.",
            "google_drive": {"uploaded": bool(drive_link), "link": drive_link},
        }
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}
