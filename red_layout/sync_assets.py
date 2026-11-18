"""Sincroniza o inventário com a planilha de licenças, ramais e DIDs."""
from __future__ import annotations

import csv
import unicodedata
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook

from red_layout.database import SessionLocal, init_db
from red_layout.models import Machine

EXCEL = Path(r"C:\Users\justi\Downloads\LICENÇAS OFFICE.xlsx")
DIDS = Path(r"C:\Users\justi\Downloads\RED PRECATORIOS E INTERMEDIACAO LTDA-dids-2026-09-10T15_42_42.296Z.csv")
DOMAIN = "redprecatorios.com.br"

RAMAIS = [
    ("Aline", "9101", "aline.aline"),
    ("Julia", "9103", "julia.barbosa"),
    ("Clauber", "9104", "clauber.souza"),
    ("Telma", "9105", "telma.santana"),
    ("Cirlene", "9106", "cirlene.pinazo"),
    ("Fernanda Cost.", "9108", "fernanda.costa"),
    ("Evely", "9109", "evely.pereira"),
    ("Filipe", "9111", ""),
    ("Amanda", "9112", "amanda.cristina"),
    ("Karina", "9113", "karina.aparecida"),
    ("Jessica", "9114", "jessica.silva"),
    ("Christopher", "9115", "christopher.santos"),
    ("Wesley Benedito", "9116", "wesley.benedito"),
    ("Juliana", "9117", "juliana.milena"),
]

JURIDICO = {
    "ana locatelli",
    "renata martins",
    "jose amaro",
    "lucas nobrega",
    "camila cuelhar",
}


def norm(text: str) -> str:
    text = unicodedata.normalize("NFD", (text or "").strip().lower())
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return " ".join(text.replace(".", " ").replace("-", " ").split())


def tokens(text: str) -> list[str]:
    return [t for t in norm(text).split() if t not in {"de", "da", "do", "das", "dos"}]


def cell(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y")
    text = str(value).strip()
    if text in {"-", "None"}:
        return ""
    return text


def looks_like_key(value: str) -> bool:
    return bool(value) and value.upper() not in {"DESATIVADO", "NAO", "NÃO", "VAGO", "PENDENTE"}


def area_for(user: str, vago: bool) -> str:
    if vago or not user:
        return "VAZIO"
    n = norm(user)
    if "sistema" in n:
        return "SISTEMA"
    if n in JURIDICO:
        return "JURIDICO"
    first = tokens(user)[0] if tokens(user) else ""
    ramal_firsts = {tokens(name)[0] for name, _, _ in RAMAIS if tokens(name)}
    if first in ramal_firsts:
        return "COMPRAS"
    return ""


def similar_last(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return a == b or a.startswith(b) or b.startswith(a)


def match_person(query: str, candidates: list[Machine]) -> Machine | None:
    q = tokens(query)
    if not q:
        return None
    scored = []
    for machine in candidates:
        u = tokens(machine.user_name)
        if not u:
            continue
        if norm(machine.user_name) == norm(query):
            scored.append((100, machine))
            continue
        if q[0] != u[0]:
            continue
        if len(q) == 1:
            scored.append((55, machine))
            continue
        if similar_last(q[-1], u[-1]) or q[-1] in u or u[-1] in q:
            scored.append((80, machine))
    if not scored:
        return None
    scored.sort(key=lambda item: item[0], reverse=True)
    best = scored[0][0]
    top = [m for s, m in scored if s == best]
    return top[0] if len(top) == 1 else None


def read_excel() -> list[dict]:
    wb = load_workbook(EXCEL, data_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = []
    for row in ws.iter_rows(min_row=4, values_only=True):
        hostname = cell(row[1] if len(row) > 1 else None)
        if not hostname:
            continue
        user = cell(row[6] if len(row) > 6 else None)
        vago = user.upper() == "VAGO" or cell(row[3] if len(row) > 3 else None).upper() == "VAGO"
        win_key = cell(row[11] if len(row) > 11 else None)
        office_keys = [cell(row[16] if len(row) > 16 else None), cell(row[17] if len(row) > 17 else None)]
        office_key = " | ".join(k for k in office_keys if k)
        win_orig = cell(row[13] if len(row) > 13 else None)
        win_atual = cell(row[14] if len(row) > 14 else None)
        win_versao = " / ".join(v for v in [win_orig, win_atual] if v) or win_orig
        win_status = cell(row[15] if len(row) > 15 else None)
        if win_key.upper() == "DESATIVADO":
            win_status = "DESATIVADO"
            win_key = ""
        rows.append({
            "hostname": hostname,
            "ip": cell(row[2] if len(row) > 2 else None),
            "observacao": "" if vago and cell(row[3]).upper() == "VAGO" else cell(row[3] if len(row) > 3 else None),
            "gpu": cell(row[4] if len(row) > 4 else None).upper() in {"SIM", "TRUE", "1"},
            "script": cell(row[5] if len(row) > 5 else None),
            "user_name": "" if vago else user,
            "area": area_for(user, vago),
            "email": cell(row[8] if len(row) > 8 else None),
            "ramal": cell(row[9] if len(row) > 9 else None),
            "numero_ind": cell(row[10] if len(row) > 10 else None),
            "win_product_key": win_key if looks_like_key(win_key) else "",
            "win_product_id": cell(row[12] if len(row) > 12 else None),
            "win_versao": win_versao,
            "win_status": win_status or "ATIVO",
            "office_key": office_key,
            "office_status": cell(row[20] if len(row) > 20 else None) or "PENDENTE",
            "ultima_atualizacao": cell(row[21] if len(row) > 21 else None),
            "politica": cell(row[22] if len(row) > 22 else None),
            "bitlocker_status": cell(row[23] if len(row) > 23 else None),
        })
    return rows


def read_dids() -> list[tuple[str, str]]:
    if not DIDS.exists():
        return []
    items = []
    with DIDS.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for row in reader:
            name = cell(row.get("Name"))
            number = cell(row.get("Number"))
            if name and number and not name.lower().startswith("livre") and name not in {"-", "permanente"}:
                items.append((name, number))
    return items


def apply_row(machine: Machine, data: dict) -> None:
    for key, value in data.items():
        setattr(machine, key, value if value is not None else "")


def sync() -> None:
    init_db()
    db = SessionLocal()
    try:
        excel_rows = read_excel()
        hostnames = {row["hostname"] for row in excel_rows}
        created = updated = 0
        for data in excel_rows:
            machine = db.query(Machine).filter(Machine.hostname == data["hostname"]).first()
            if machine:
                apply_row(machine, data)
                updated += 1
            else:
                db.add(Machine(**data))
                created += 1
        db.flush()

        removed = 0
        for machine in db.query(Machine).all():
            if machine.hostname not in hostnames and not machine.hostname.startswith("RAMAL-"):
                machine.bay_id = None
                db.delete(machine)
                removed += 1
        db.flush()

        all_machines = db.query(Machine).all()
        real_pcs = [m for m in all_machines if not m.hostname.startswith("RAMAL-")]
        for extra in [m for m in all_machines if m.hostname.startswith("RAMAL-")]:
            twin = match_person(extra.user_name, real_pcs)
            if not twin:
                continue
            if extra.ramal and not twin.ramal:
                twin.ramal = extra.ramal
            if extra.email and not twin.email:
                twin.email = extra.email
            if extra.numero_ind and not twin.numero_ind:
                twin.numero_ind = extra.numero_ind
            extra.bay_id = None
            db.delete(extra)
        db.flush()

        people = [m for m in db.query(Machine).all() if m.user_name]
        ramal_hits = 0
        ramal_created = 0
        for name, ramal, login in RAMAIS:
            machine = match_person(name, people)
            if not machine:
                hostname = f"RAMAL-{ramal}"
                machine = db.query(Machine).filter(Machine.hostname == hostname).first()
                if not machine:
                    display = name.replace("Cost.", "Costa").strip()
                    machine = Machine(
                        hostname=hostname,
                        user_name=display,
                        area="COMPRAS",
                        email=f"{login}@{DOMAIN}" if login else "",
                        observacao="Ramal sem PC na planilha de licenças",
                    )
                    db.add(machine)
                    db.flush()
                    ramal_created += 1
                    people.append(machine)
            machine.ramal = ramal
            if login and not machine.email:
                machine.email = f"{login}@{DOMAIN}"
            if not machine.area:
                machine.area = "COMPRAS"
            ramal_hits += 1

        did_hits = 0
        for name, number in read_dids():
            machine = match_person(name, people)
            if not machine:
                continue
            machine.numero_ind = number
            did_hits += 1

        db.commit()
        print(f"Máquinas da planilha: {len(excel_rows)}")
        print(f"Criadas: {created} | Atualizadas: {updated} | Removidas (seed antigo): {removed}")
        print(f"Ramais aplicados: {ramal_hits} (novos cadastros sem PC: {ramal_created}) | Telefones DID aplicados: {did_hits}")
        unmatched_ramal = []
        people = [m for m in db.query(Machine).all() if m.user_name]
        for name, ramal, _ in RAMAIS:
            if not match_person(name, people):
                unmatched_ramal.append(f"{name} ({ramal})")
        if unmatched_ramal:
            print("Ramais sem máquina na planilha:", ", ".join(unmatched_ramal))
    finally:
        db.close()


if __name__ == "__main__":
    sync()
