import re
import sys
import time
import unicodedata
from datetime import date, datetime


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


def _normalize_date_text(value) -> str:
    """Texto de data limpo; vazio quando NULL / placeholder / inválido para a planilha."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y")
    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    text = " ".join(str(value).strip().split())
    if not text:
        return ""
    ascii_fold = (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    if ascii_fold in {"none", "null", "nan"}:
        return ""
    if "nao informado" in ascii_fold:
        return ""
    return text


def normalize_db_date_str(value) -> str:
    """
    Converte valor vindo do MySQL (Data_Base / Data_Decisao) para string da planilha.
    NULL, vazio ou «Não informado…» → string vazia (não usar str(None) == 'None').
    """
    return _normalize_date_text(value)


def parse_planilha_date(value):
    """
    Converte Data_Base / Data_Inscrição para ``datetime`` (só data) ou ``None``.

    ``None`` deve ser gravado na célula (em branco), para não deixar a data
    default do template e evitar cálculo indevido.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    text = _normalize_date_text(value)
    if not text:
        return None

    # Remove hora se vier colada (ex.: 25/06/2020 00:00:00)
    text_date = text.split()[0] if " " in text else text
    text_date = text_date.replace(".", "/").replace("-", "/")

    for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%d/%m/%y"):
        try:
            dt = datetime.strptime(text_date, fmt)
            return datetime(dt.year, dt.month, dt.day)
        except ValueError:
            continue

    # dd/mm/yyyy com zeros opcionais
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", text_date)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d)
        except ValueError:
            return None
    return None

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