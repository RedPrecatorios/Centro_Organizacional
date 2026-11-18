from __future__ import annotations

from datetime import datetime
from io import StringIO
import csv

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from sqlalchemy.orm import Session, joinedload

from red_layout.database import get_db, init_db
from red_layout.goto import account_info as goto_account_info
from red_layout.goto import create_user as goto_create_user
from red_layout.goto import find_user_by_email as goto_find_user
from red_layout.goto import inherit_phone_number as goto_inherit_phone
from red_layout.goto import list_account_phones as goto_list_phones
from red_layout.goto import list_users as goto_list_users
from red_layout.goto import reactivate_user as goto_reactivate_user
from red_layout.goto import suspend_user as goto_suspend_user
from red_layout.goto import user_missing_error as goto_user_missing
from red_layout.monday import account_info as monday_account_info
from red_layout.monday import provision_user as monday_provision_user
from red_layout.plataforma import account_info as plataforma_account_info
from red_layout.plataforma import provision_user as plataforma_provision_user
from red_layout.syscall import account_info as syscall_account_info
from red_layout.syscall import delete_user as syscall_delete_user
from red_layout.syscall import manual_result as syscall_manual
from red_layout.syscall import skipped_result as syscall_skipped
from red_layout.models import Bay, GoToExcludedUser, GoToPhoneNumber, GoToUserHistory, GoToUserProfile, Machine, utcnow
from red_layout.schemas import (
    AssignBody,
    BayNotesUpdate,
    BayOut,
    GoToHistoryOut,
    GoToPhoneListOut,
    GoToPhoneOut,
    GoToStatusOut,
    GoToUserContact,
    GoToUserCreate,
    GoToUserLink,
    GoToUserLookupOut,
    GoToUserOut,
    MondayStatusOut,
    PlataformaStatusOut,
    SyscallStatusOut,
    MachineCreate,
    MachineOut,
    MachineUpdate,
    StatsOut,
)
from red_layout.seed import ensure_bays, normalize_empty_labels, seed_if_empty

ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"
load_dotenv(ROOT / ".env")

CSV_FIELDS = [
    ("HOSTNAME", "hostname"),
    ("IP", "ip"),
    ("OBSERVACAO", "observacao"),
    ("GPU", "gpu"),
    ("SCRIPT", "script"),
    ("USER", "user_name"),
    ("AREA", "area"),
    ("EMAIL", "email"),
    ("RAMAL", "ramal"),
    ("NUMERO IND", "numero_ind"),
    ("WIN PRODUCT KEY", "win_product_key"),
    ("WIN PRODUCT ID", "win_product_id"),
    ("VERSAO ORIGINAL", "win_versao"),
    ("WIN STATUS", "win_status"),
    ("OFFICE KEY", "office_key"),
    ("OFFICE STATUS", "office_status"),
    ("ULTIMA ATUALIZACAO", "ultima_atualizacao"),
    ("POLITICA", "politica"),
    ("BITLOCKER", "bitlocker_status"),
    ("BAIA", "bay_id"),
]


app = FastAPI(title="RED Layout", version="1.0.0")


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    db = next(get_db())
    try:
        ensure_bays(db)
        seed_if_empty(db)
        normalize_empty_labels(db)
        _backfill_history(db)
        _backfill_machine_phones(db)
    finally:
        db.close()


def machine_to_out(machine: Machine) -> MachineOut:
    hostname = (machine.hostname or "").strip() or f"PC-{machine.id}"
    original = machine.hostname
    machine.hostname = hostname
    try:
        data = MachineOut.model_validate(machine)
        return data.model_copy(update={"bay_id": machine.bay_id})
    finally:
        machine.hostname = original


def bay_to_out(bay: Bay) -> BayOut:
    machines = [machine_to_out(m) for m in bay.machines]
    return BayOut(
        id=bay.id,
        zone=bay.zone,
        label=bay.label,
        notes=bay.notes or "",
        machines=machines,
        machine=machines[0] if machines else None,
    )


def unassign_machine(db: Session, machine_id: int) -> None:
    machine = db.get(Machine, machine_id)
    if machine:
        machine.bay_id = None


def place_machine(db: Session, machine: Machine, bay_id: str | None) -> None:
    if not bay_id:
        machine.bay_id = None
        return
    bay = db.get(Bay, bay_id)
    if not bay:
        raise HTTPException(404, "Baia não encontrada")
    machine.bay_id = bay.id


@app.get("/api/bays", response_model=list[BayOut])
def list_bays(db: Session = Depends(get_db)):
    bays = (
        db.query(Bay)
        .options(joinedload(Bay.machines))
        .order_by(Bay.id)
        .all()
    )
    return [bay_to_out(b) for b in bays]


@app.get("/api/bays/{bay_id}", response_model=BayOut)
def get_bay(bay_id: str, db: Session = Depends(get_db)):
    bay = db.query(Bay).options(joinedload(Bay.machines)).filter(Bay.id == bay_id).first()
    if not bay:
        raise HTTPException(404, "Baia não encontrada")
    return bay_to_out(bay)


@app.put("/api/bays/{bay_id}/notes", response_model=BayOut)
def update_bay_notes(bay_id: str, body: BayNotesUpdate, db: Session = Depends(get_db)):
    bay = db.query(Bay).options(joinedload(Bay.machines)).filter(Bay.id == bay_id).first()
    if not bay:
        raise HTTPException(404, "Baia não encontrada")
    bay.notes = body.notes
    db.commit()
    db.refresh(bay)
    return bay_to_out(bay)


@app.put("/api/bays/{bay_id}/assign", response_model=BayOut)
def assign_bay(bay_id: str, body: AssignBody, db: Session = Depends(get_db)):
    bay = db.query(Bay).options(joinedload(Bay.machines)).filter(Bay.id == bay_id).first()
    if not bay:
        raise HTTPException(404, "Baia não encontrada")

    if body.machine_id is None:
        for machine in list(bay.machines):
            machine.bay_id = None
        db.commit()
        db.refresh(bay)
        return bay_to_out(bay)

    machine = db.get(Machine, body.machine_id)
    if not machine:
        raise HTTPException(404, "Máquina não encontrada")
    place_machine(db, machine, bay.id)
    db.commit()
    db.refresh(bay)
    return bay_to_out(bay)


@app.get("/api/machines", response_model=list[MachineOut])
def list_machines(db: Session = Depends(get_db)):
    machines = db.query(Machine).options(joinedload(Machine.bay)).order_by(Machine.hostname).all()
    return [machine_to_out(m) for m in machines]


@app.get("/api/machines/{machine_id}", response_model=MachineOut)
def get_machine(machine_id: int, db: Session = Depends(get_db)):
    machine = db.query(Machine).options(joinedload(Machine.bay)).filter(Machine.id == machine_id).first()
    if not machine:
        raise HTTPException(404, "Máquina não encontrada")
    return machine_to_out(machine)


@app.post("/api/machines", response_model=MachineOut, status_code=201)
def create_machine(body: MachineCreate, db: Session = Depends(get_db)):
    exists = db.query(Machine).filter(Machine.hostname == body.hostname).first()
    if exists:
        raise HTTPException(400, "Já existe uma máquina com esse hostname")
    data = body.model_dump(exclude={"bay_id"})
    machine = Machine(**data)
    db.add(machine)
    db.flush()
    if body.bay_id:
        place_machine(db, machine, body.bay_id)
    db.commit()
    db.refresh(machine)
    return machine_to_out(machine)


@app.put("/api/machines/{machine_id}", response_model=MachineOut)
def update_machine(machine_id: int, body: MachineUpdate, db: Session = Depends(get_db)):
    machine = db.query(Machine).options(joinedload(Machine.bay)).filter(Machine.id == machine_id).first()
    if not machine:
        raise HTTPException(404, "Máquina não encontrada")
    payload = body.model_dump(exclude_unset=True)
    bay_id = payload.pop("bay_id", "__skip__")
    if "hostname" in payload:
        payload["hostname"] = (payload["hostname"] or "").strip()
        if not payload["hostname"]:
            raise HTTPException(400, "Informe o hostname")
    if "hostname" in payload and payload["hostname"] != machine.hostname:
        clash = db.query(Machine).filter(Machine.hostname == payload["hostname"]).first()
        if clash:
            raise HTTPException(400, "Já existe uma máquina com esse hostname")
    for key, value in payload.items():
        setattr(machine, key, value)
    if bay_id != "__skip__":
        place_machine(db, machine, bay_id)
    db.commit()
    db.refresh(machine)
    return machine_to_out(machine)


@app.delete("/api/machines/{machine_id}", status_code=204)
def delete_machine(machine_id: int, db: Session = Depends(get_db)):
    machine = db.get(Machine, machine_id)
    if not machine:
        raise HTTPException(404, "Máquina não encontrada")
    goto_key = (machine.goto_user_key or "").strip()
    email = (machine.email or "").strip()
    if goto_key or email:
        try:
            if not goto_key and email:
                found = goto_find_user(email)
                goto_key = (found or {}).get("user_key") or ""
            if goto_key:
                _archive_goto_user(db, goto_key, email)
                _dismantle_goto_phone(db, goto_key, email)
                goto_suspend_user(goto_key)
                _exclude_goto_user(db, goto_key, email)
        except RuntimeError as exc:
            raise HTTPException(502, f"Não foi possível suspender o usuário na GoTo: {exc}") from exc
        _remove_syscall_user(email)
    unassign_machine(db, machine.id)
    db.delete(machine)
    db.commit()


@app.get("/api/goto/status", response_model=GoToStatusOut)
def goto_status():
    try:
        info = goto_account_info()
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    return GoToStatusOut(ok=True, **info)


@app.get("/api/monday/status", response_model=MondayStatusOut)
def monday_status():
    try:
        info = monday_account_info()
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    return MondayStatusOut(ok=True, **info)


@app.get("/api/plataforma/status", response_model=PlataformaStatusOut)
def plataforma_status():
    try:
        info = plataforma_account_info()
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    return PlataformaStatusOut(ok=True, **info)


@app.get("/api/syscall/status", response_model=SyscallStatusOut)
def syscall_status():
    try:
        info = syscall_account_info()
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    return SyscallStatusOut(ok=True, **info)


def _format_phone_label(number: str, name: str = "") -> str:
    digits = "".join(ch for ch in (number or "") if ch.isdigit())
    pretty = number
    if digits.startswith("55") and len(digits) >= 12:
        local = digits[2:]
        if len(local) == 10:
            pretty = f"({local[:2]}) {local[2:6]}-{local[6:]}"
        elif len(local) == 11:
            pretty = f"({local[:2]}) {local[2:7]}-{local[7:]}"
    if name:
        return f"{pretty} · {name}"
    return pretty


def _sync_phone_catalog(db: Session) -> list[GoToPhoneNumber]:
    fetched = goto_list_phones()
    keep = {item["id"] for item in fetched}
    for row in db.query(GoToPhoneNumber).all():
        if row.id not in keep:
            db.delete(row)
    for item in fetched:
        row = db.get(GoToPhoneNumber, item["id"])
        if not row:
            db.add(GoToPhoneNumber(
                id=item["id"],
                number=item["number"],
                name=item["name"],
                status=item["status"],
            ))
            continue
        row.number = item["number"]
        row.name = item["name"]
        row.status = item["status"]
    db.flush()
    return db.query(GoToPhoneNumber).order_by(GoToPhoneNumber.number).all()


def _phones_out(db: Session, rows: list[GoToPhoneNumber]) -> list[GoToPhoneOut]:
    holders: dict[str, tuple[str, str]] = {}
    for profile in db.query(GoToUserProfile).all():
        phone = (profile.phone or "").strip()
        if phone:
            holders[phone] = (profile.user_key, (profile.email or "").strip())
    names_by_email = {
        (machine.email or "").strip().lower(): (machine.user_name or "").strip()
        for machine in db.query(Machine).all()
        if (machine.email or "").strip()
    }
    items: list[GoToPhoneOut] = []
    for row in rows:
        user_key, email = holders.get(row.number, ("", ""))
        assigned_to = names_by_email.get(email.lower(), "") if email else ""
        if not assigned_to and email:
            assigned_to = email.split("@")[0].replace(".", " ")
        items.append(GoToPhoneOut(
            id=row.id,
            number=row.number,
            name=row.name or "",
            status=row.status or "",
            label=_format_phone_label(row.number, row.name),
            assigned_to=assigned_to,
            assigned_user_key=user_key,
        ))
    return items


@app.get("/api/goto/phones", response_model=GoToPhoneListOut)
def list_goto_phones(db: Session = Depends(get_db)):
    rows = db.query(GoToPhoneNumber).order_by(GoToPhoneNumber.number).all()
    if not rows:
        try:
            rows = _sync_phone_catalog(db)
            db.commit()
        except RuntimeError as exc:
            raise HTTPException(502, str(exc)) from exc
    return GoToPhoneListOut(count=len(rows), phones=_phones_out(db, rows))


@app.post("/api/goto/phones/refresh", response_model=GoToPhoneListOut)
def refresh_goto_phones(db: Session = Depends(get_db)):
    try:
        rows = _sync_phone_catalog(db)
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    db.commit()
    return GoToPhoneListOut(count=len(rows), phones=_phones_out(db, rows))


def _phone_index(db: Session) -> tuple[dict[str, str], dict[str, str]]:
    by_key: dict[str, str] = {}
    by_email: dict[str, str] = {}
    for row in db.query(GoToUserProfile).all():
        phone = (row.phone or "").strip()
        if row.user_key:
            by_key[row.user_key] = phone
        email = (row.email or "").strip().lower()
        if email:
            by_email[email] = phone
    return by_key, by_email


def _phone_for(by_key: dict[str, str], by_email: dict[str, str], user_key: str, email: str) -> str:
    return by_key.get(user_key) or by_email.get((email or "").strip().lower()) or ""


def _display_name(first_name: str = "", last_name: str = "", email: str = "") -> str:
    name = " ".join(part for part in ((first_name or "").strip(), (last_name or "").strip()) if part)
    if name:
        return name
    local = (email or "").split("@")[0].replace(".", " ").strip()
    return local


def _inherit_phone(
    db: Session,
    user_key: str,
    phone: str,
    first_name: str = "",
    last_name: str = "",
    email: str = "",
    wait_for_line: bool = False,
) -> str:
    previous = ""
    row = _get_goto_profile(db, user_key)
    if row:
        previous = (row.phone or "").strip()
    inherited = goto_inherit_phone(
        user_key,
        phone,
        display_name=_display_name(first_name, last_name, email),
        previous_number=previous,
        wait_for_line=wait_for_line,
    )
    phone_id = str(inherited.get("id") or "")
    if phone_id:
        catalog = db.get(GoToPhoneNumber, phone_id)
        if catalog:
            catalog.name = str(inherited.get("name") or catalog.name or "")
    elif previous:
        label = f"Livre - (Antiga {_display_name(first_name, last_name, email)})"
        wanted = "".join(ch for ch in previous if ch.isdigit())
        for item in db.query(GoToPhoneNumber).all():
            if "".join(ch for ch in (item.number or "") if ch.isdigit()) == wanted:
                item.name = label
                break
    return (phone or "").strip()


def _goto_user_names(user_key: str, email: str = "") -> tuple[str, str, str]:
    user_key = (user_key or "").strip()
    email_norm = (email or "").strip().lower()
    match = None
    try:
        users = goto_list_users()
    except RuntimeError:
        users = []
    for user in users:
        if user_key and user.get("user_key") == user_key:
            match = user
            break
        if email_norm and (user.get("email") or "").strip().lower() == email_norm:
            match = user
    if match is None and email_norm:
        try:
            match = goto_find_user(email_norm)
        except RuntimeError:
            match = None
    if not match:
        return "", "", email
    return (
        str(match.get("first_name") or ""),
        str(match.get("last_name") or ""),
        str(match.get("email") or email),
    )


def _dismantle_goto_phone(db: Session, user_key: str, email: str = "") -> None:
    user_key = (user_key or "").strip()
    if not user_key:
        return
    row = db.get(GoToUserProfile, user_key)
    if not row or not (row.phone or "").strip():
        return
    first_name, last_name, email = _goto_user_names(user_key, email or row.email or "")
    try:
        _inherit_phone(db, user_key, "", first_name, last_name, email)
    except RuntimeError as exc:
        if not goto_user_missing(exc):
            raise
    _save_phone(db, user_key, email, "", first_name, last_name)


def _archive_goto_user(db: Session, user_key: str, email: str = "", excluded_at: datetime | None = None) -> None:
    user_key = (user_key or "").strip()
    if not user_key:
        return
    profile = db.get(GoToUserProfile, user_key)
    first_name = (profile.first_name if profile else "") or ""
    last_name = (profile.last_name if profile else "") or ""
    email = email or (profile.email if profile else "") or ""
    if not first_name or not last_name:
        api_first, api_last, api_email = _goto_user_names(user_key, email)
        first_name = first_name or api_first
        last_name = last_name or api_last
        email = email or api_email
    machine = next(
        (item for item in db.query(Machine).filter(Machine.goto_user_key == user_key).all()),
        None,
    )
    if machine is None and email:
        machine = next(
            (
                item
                for item in db.query(Machine).all()
                if (item.email or "").strip().lower() == email.strip().lower()
            ),
            None,
        )
    entered = profile.entered_at if profile else None
    if entered is None and machine is not None:
        entered = machine.created_at
    area = ""
    if profile and (profile.area or "").strip():
        area = profile.area.strip().upper()
    elif machine and (machine.area or "").strip():
        area = machine.area.strip().upper()
    db.add(
        GoToUserHistory(
            user_key=user_key,
            email=email,
            first_name=first_name,
            last_name=last_name,
            phone=(profile.phone if profile else "") or (machine.numero_ind if machine else "") or "",
            area=area,
            hostname=(machine.hostname if machine else "") or "",
            bay_id=(machine.bay_id if machine else "") or "",
            entered_at=entered,
            excluded_at=excluded_at or utcnow(),
        )
    )


def _backfill_history(db: Session) -> None:
    existing = {
        (row.user_key, (row.email or "").strip().lower())
        for row in db.query(GoToUserHistory).all()
    }
    dirty = False
    for row in db.query(GoToExcludedUser).all():
        marker = (row.user_key, (row.email or "").strip().lower())
        if marker in existing:
            continue
        _archive_goto_user(db, row.user_key, row.email or "", excluded_at=row.excluded_at)
        existing.add(marker)
        dirty = True
    profiles = {row.user_key: row for row in db.query(GoToUserProfile).all()}
    by_email = {
        (row.email or "").strip().lower(): row
        for row in profiles.values()
        if (row.email or "").strip()
    }
    for row in db.query(GoToUserHistory).all():
        profile = profiles.get(row.user_key) or by_email.get((row.email or "").strip().lower())
        if not profile:
            continue
        if not (row.first_name or "").strip() and profile.first_name:
            row.first_name = profile.first_name
            dirty = True
        if not (row.last_name or "").strip() and profile.last_name:
            row.last_name = profile.last_name
            dirty = True
        if not (row.email or "").strip() and profile.email:
            row.email = profile.email
            dirty = True
        if not (row.phone or "").strip() and profile.phone:
            row.phone = profile.phone
            dirty = True
        if not (row.area or "").strip() and profile.area:
            row.area = profile.area
            dirty = True
        if row.entered_at is None and profile.entered_at is not None:
            row.entered_at = profile.entered_at
            dirty = True
    if dirty:
        db.commit()


def _save_phone(
    db: Session,
    user_key: str,
    email: str,
    phone: str,
    first_name: str = "",
    last_name: str = "",
) -> str:
    user_key = (user_key or "").strip()
    email = (email or "").strip()
    phone = (phone or "").strip()
    if not user_key:
        raise HTTPException(400, "Usuário GoTo inválido")
    if phone:
        for other in db.query(GoToUserProfile).filter(
            GoToUserProfile.phone == phone,
            GoToUserProfile.user_key != user_key,
        ).all():
            other.phone = ""
    _touch_goto_profile(db, user_key, email, first_name, last_name, phone)
    _sync_machine_phone(db, user_key, phone)
    return phone


def _sync_machine_phone(db: Session, user_key: str, phone: str) -> None:
    user_key = (user_key or "").strip()
    phone = (phone or "").strip()
    if not user_key or not phone:
        return
    for machine in db.query(Machine).filter(Machine.goto_user_key == user_key).all():
        machine.numero_ind = phone


def _profile_phone(db: Session, user_key: str = "", email: str = "") -> str:
    profile = _get_goto_profile(db, user_key) if user_key else None
    if profile and (profile.phone or "").strip():
        return profile.phone.strip()
    email_norm = (email or "").strip().lower()
    if not email_norm:
        return ""
    for row in db.query(GoToUserProfile).filter(GoToUserProfile.email != "").all():
        if (row.email or "").strip().lower() != email_norm:
            continue
        if (row.phone or "").strip():
            return row.phone.strip()
    return ""


def _backfill_machine_phones(db: Session) -> None:
    by_key: dict[str, str] = {}
    by_email: dict[str, str] = {}
    for row in db.query(GoToUserProfile).all():
        phone = (row.phone or "").strip()
        if not phone:
            continue
        if row.user_key:
            by_key[row.user_key] = phone
        email = (row.email or "").strip().lower()
        if email:
            by_email[email] = phone
    dirty = False
    for machine in db.query(Machine).all():
        if (machine.numero_ind or "").strip():
            continue
        phone = by_key.get((machine.goto_user_key or "").strip()) or by_email.get(
            (machine.email or "").strip().lower()
        )
        if phone:
            machine.numero_ind = phone
            dirty = True
    if dirty:
        db.commit()


def _get_goto_profile(db: Session, user_key: str) -> GoToUserProfile | None:
    user_key = str(user_key or "").strip()
    if not user_key:
        return None
    for obj in list(db.new) + list(db.identity_map.values()):
        if isinstance(obj, GoToUserProfile) and str(obj.user_key) == user_key:
            return obj
    return (
        db.query(GoToUserProfile)
        .filter(GoToUserProfile.user_key == user_key)
        .first()
    )


def _touch_goto_profile(
    db: Session,
    user_key: str,
    email: str = "",
    first_name: str = "",
    last_name: str = "",
    phone: str | None = None,
    status: str = "",
    area: str = "",
) -> GoToUserProfile:
    user_key = str(user_key or "").strip()
    if not user_key:
        raise HTTPException(400, "Usuário GoTo inválido")
    row = _get_goto_profile(db, user_key)
    if row is None:
        row = GoToUserProfile(
            user_key=user_key,
            email=email,
            first_name=first_name,
            last_name=last_name,
            phone=phone or "",
            status=status or ("LOCAL" if _is_local_user_key(user_key) else ""),
            area=(area or "").strip().upper(),
            entered_at=utcnow(),
        )
        db.add(row)
        return row
    if email:
        row.email = email
    if first_name:
        row.first_name = first_name
    if last_name:
        row.last_name = last_name
    if phone is not None:
        row.phone = phone
    if status:
        row.status = status
    if area:
        row.area = area.strip().upper()
    if row.entered_at is None:
        row.entered_at = utcnow()
    row.updated_at = utcnow()
    return row


def _goto_user_out(
    user: dict,
    machines: list[Machine],
    by_key: dict[str, str] | None = None,
    by_email: dict[str, str] | None = None,
) -> GoToUserOut:
    key = str(user.get("user_key") or "")
    email = (user.get("email") or "").strip().lower()
    match = None
    if key:
        match = next((m for m in machines if (m.goto_user_key or "") == key), None)
    if match is None and email:
        match = next((m for m in machines if (m.email or "").strip().lower() == email), None)
    phone = user.get("phone")
    if phone is None:
        phone = _phone_for(by_key or {}, by_email or {}, key, email)
    return GoToUserOut(
        user_key=key,
        email=user.get("email") or "",
        first_name=user.get("first_name") or "",
        last_name=user.get("last_name") or "",
        status=user.get("status") or "",
        can_suspend=bool(user.get("can_suspend")),
        account_key=user.get("account_key") or "",
        machine_id=match.id if match else None,
        hostname=match.hostname if match else "",
        bay_id=match.bay_id if match else None,
        user_name=match.user_name if match else "",
        phone=str(phone or ""),
        existed=bool(user.get("existed")),
        monday_invited=bool(user.get("monday_invited")),
        monday_existed=bool(user.get("monday_existed")),
        monday_team=user.get("monday_team") or "",
        monday_column=user.get("monday_column") or "",
        monday_label=user.get("monday_label") or "",
        monday_label_created=bool(user.get("monday_label_created")),
        monday_warning=user.get("monday_warning") or "",
        plataforma_created=bool(user.get("plataforma_created")),
        plataforma_existed=bool(user.get("plataforma_existed")),
        plataforma_username=user.get("plataforma_username") or "",
        plataforma_password=user.get("plataforma_password") or "",
        plataforma_warning=user.get("plataforma_warning") or "",
        syscall_created=bool(user.get("syscall_created")),
        syscall_existed=bool(user.get("syscall_existed")),
        syscall_skipped=bool(user.get("syscall_skipped")),
        syscall_username=user.get("syscall_username") or "",
        syscall_password=user.get("syscall_password") or "",
        syscall_deleted=bool(user.get("syscall_deleted")),
        syscall_warning=user.get("syscall_warning") or "",
        goto_warning=user.get("goto_warning") or "",
    )


def _link_goto_user(
    db: Session,
    user_key: str,
    email: str,
    first_name: str,
    last_name: str,
    machine_id: int | None,
    area: str = "",
) -> Machine | None:
    full_name = " ".join(part for part in [first_name.strip(), last_name.strip()] if part)
    if user_key:
        for machine in db.query(Machine).filter(Machine.goto_user_key == user_key).all():
            if machine_id is None or machine.id != machine_id:
                machine.goto_user_key = ""
        _touch_goto_profile(db, user_key, email, first_name, last_name, area=area)
    if machine_id is None:
        return None
    machine = db.get(Machine, machine_id)
    if not machine:
        raise HTTPException(404, "Máquina não encontrada")
    if user_key:
        machine.goto_user_key = user_key
    if email:
        machine.email = email.strip()
    if full_name:
        machine.user_name = full_name
    if area:
        machine.area = area.strip().upper()
    if user_key:
        _touch_goto_profile(db, user_key, email, first_name, last_name, area=area)
        phone = _profile_phone(db, user_key, email)
        if phone:
            machine.numero_ind = phone
    return machine


def _excluded_keys(db: Session) -> set[str]:
    return {row.user_key for row in db.query(GoToExcludedUser).all() if row.user_key}


def _excluded_emails(db: Session) -> set[str]:
    return {
        (row.email or "").strip().lower()
        for row in db.query(GoToExcludedUser).all()
        if (row.email or "").strip()
    }


def _local_user_key(email: str) -> str:
    return f"local:{(email or '').strip().lower()}"


def _is_local_user_key(user_key: str) -> bool:
    return (user_key or "").startswith("local:")


def _release_user_machines(db: Session, user_key: str, email: str = "") -> None:
    user_key = (user_key or "").strip()
    email_norm = (email or "").strip().lower()
    seen: set[int] = set()
    machines: list[Machine] = []
    if user_key:
        machines.extend(db.query(Machine).filter(Machine.goto_user_key == user_key).all())
    if email_norm:
        for machine in db.query(Machine).all():
            if (machine.email or "").strip().lower() == email_norm:
                machines.append(machine)
    for machine in machines:
        if machine.id in seen:
            continue
        seen.add(machine.id)
        machine.user_name = ""
        machine.email = ""
        machine.ramal = ""
        machine.numero_ind = ""
        machine.goto_user_key = ""
        machine.area = "OCIOSO" if machine.bay_id else "VAZIO"


def _exclude_goto_user(db: Session, user_key: str, email: str = "") -> None:
    user_key = (user_key or "").strip()
    email_norm = (email or "").strip().lower()
    if not user_key and email_norm:
        user_key = _local_user_key(email_norm)
    if not user_key:
        return
    for machine in db.query(Machine).filter(Machine.goto_user_key == user_key).all():
        machine.goto_user_key = ""
    row = db.get(GoToExcludedUser, user_key)
    if row:
        if email_norm:
            row.email = email_norm
        return
    db.add(GoToExcludedUser(user_key=user_key, email=email_norm))


def _restore_goto_user(db: Session, user_key: str, email: str = "") -> None:
    user_key = (user_key or "").strip()
    email_norm = (email or "").strip().lower()
    rows = []
    for row in db.query(GoToExcludedUser).all():
        if user_key and row.user_key == user_key:
            rows.append(row)
            continue
        stored = (row.email or "").strip().lower()
        if email_norm and stored == email_norm:
            rows.append(row)
            continue
        if email_norm and row.user_key == _local_user_key(email_norm):
            rows.append(row)
    seen: set[str] = set()
    for row in rows:
        marker = row.user_key
        if marker in seen:
            continue
        seen.add(marker)
        db.delete(row)


def _persist_goto_users(
    db: Session,
    users: list[dict],
    hidden_keys: set[str],
    hidden_emails: set[str],
) -> None:
    machines = db.query(Machine).all()
    by_key = {(item.goto_user_key or "").strip(): item for item in machines if (item.goto_user_key or "").strip()}
    by_email = {
        (item.email or "").strip().lower(): item
        for item in machines
        if (item.email or "").strip()
    }
    for user in users:
        key = str(user.get("user_key") or "").strip()
        email = (user.get("email") or "").strip()
        email_norm = email.lower()
        if not key:
            continue
        if key in hidden_keys or (email_norm and email_norm in hidden_emails):
            continue
        machine = by_key.get(key) or (by_email.get(email_norm) if email_norm else None)
        existing = _get_goto_profile(db, key)
        seed_phone = (machine.numero_ind or "").strip() if machine else ""
        _touch_goto_profile(
            db,
            key,
            email,
            user.get("first_name") or "",
            user.get("last_name") or "",
            phone=seed_phone if seed_phone and not existing else None,
            status=user.get("status") or "",
            area=(machine.area if machine else "") or "",
        )


@app.get("/api/goto/users", response_model=list[GoToUserOut])
def list_goto_users(db: Session = Depends(get_db)):
    hidden_keys = _excluded_keys(db)
    hidden_emails = _excluded_emails(db)
    try:
        remote = goto_list_users()
        _persist_goto_users(db, remote, hidden_keys, hidden_emails)
        db.commit()
    except RuntimeError:
        pass
    machines = db.query(Machine).all()
    by_key, by_email = _phone_index(db)
    visible: list[GoToUserOut] = []
    seen: set[str] = set()
    for profile in db.query(GoToUserProfile).all():
        email = (profile.email or "").strip().lower()
        if profile.user_key in hidden_keys or (email and email in hidden_emails):
            continue
        marker = email or profile.user_key
        if marker in seen:
            continue
        seen.add(marker)
        visible.append(
            _goto_user_out(
                {
                    "user_key": profile.user_key,
                    "email": profile.email,
                    "first_name": profile.first_name,
                    "last_name": profile.last_name,
                    "status": profile.status or ("LOCAL" if _is_local_user_key(profile.user_key) else "ATIVO"),
                    "can_suspend": True,
                    "phone": profile.phone or "",
                },
                machines,
                by_key,
                by_email,
            )
        )
    visible.sort(key=lambda item: ((item.first_name or "").lower(), (item.last_name or "").lower(), (item.email or "").lower()))
    return visible


@app.get("/api/goto/users/lookup", response_model=GoToUserLookupOut)
def lookup_goto_user(email: str, db: Session = Depends(get_db)):
    if "@" not in (email or ""):
        raise HTTPException(400, "Informe um e-mail válido")
    try:
        found = goto_find_user(email)
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    if not found:
        return GoToUserLookupOut(found=False)
    machines = db.query(Machine).all()
    by_key, by_email = _phone_index(db)
    user = _goto_user_out(found, machines, by_key, by_email)
    return GoToUserLookupOut(found=True, user=user.model_copy(update={"existed": True}))


def _monday_fields(result: dict) -> dict:
    return {
        "monday_invited": bool(result.get("invited")),
        "monday_existed": bool(result.get("existed")),
        "monday_team": result.get("team") or "",
        "monday_column": result.get("column") or "",
        "monday_label": result.get("label") or "",
        "monday_label_created": bool(result.get("label_created")),
        "monday_warning": result.get("warning") or "",
    }


def _plataforma_fields(result: dict) -> dict:
    return {
        "plataforma_created": bool(result.get("created")),
        "plataforma_existed": bool(result.get("existed")),
        "plataforma_username": result.get("username") or "",
        "plataforma_password": result.get("password") or "",
        "plataforma_warning": result.get("warning") or "",
    }


def _syscall_fields(result: dict) -> dict:
    return {
        "syscall_created": bool(result.get("created")),
        "syscall_existed": bool(result.get("existed")),
        "syscall_skipped": bool(result.get("skipped")),
        "syscall_username": result.get("username") or "",
        "syscall_password": result.get("password") or "",
        "syscall_warning": result.get("warning") or "",
    }


def _remove_syscall_user(email: str) -> dict:
    try:
        result = syscall_delete_user(email)
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    return {
        "syscall_deleted": bool(result.get("deleted")),
        "syscall_username": result.get("username") or "",
        "syscall_warning": result.get("warning") or "",
    }


def _done_suffix(*parts: str) -> str:
    done = [part for part in parts if part]
    if not done:
        return ""
    names = " e ".join(done)
    verb = "já foram atualizados." if len(done) > 1 else "já foi atualizado."
    return f" {names} {verb}"


@app.post("/api/goto/users", response_model=GoToUserOut)
def create_goto_user(body: GoToUserCreate, db: Session = Depends(get_db)):
    email = body.email.strip()
    first_name = body.first_name.strip()
    last_name = body.last_name.strip()
    area = body.area.strip().upper()
    if "@" not in email:
        raise HTTPException(400, "Informe um e-mail válido")
    if not first_name or not last_name:
        raise HTTPException(400, "Nome e sobrenome são obrigatórios")
    if area not in {"COMPRAS", "JURIDICO", "CALCULO"}:
        raise HTTPException(400, "Escolha a área: Comprador, Jurídico ou Calculista")
    try:
        plataforma = plataforma_provision_user(email, first_name, last_name)
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    try:
        monday = monday_provision_user(email, first_name, last_name, area)
    except RuntimeError as exc:
        raise HTTPException(502, f"{exc} Plataforma Central já cadastrou o usuário.") from exc
    done = ["Plataforma Central", "Monday"]
    if area == "COMPRAS":
        syscall = syscall_manual(email, first_name, last_name)
    else:
        syscall = syscall_skipped()
    extra = {**_monday_fields(monday), **_plataforma_fields(plataforma), **_syscall_fields(syscall)}
    already = _done_suffix(*done)
    _restore_goto_user(db, "", email)
    if not body.enable_goto:
        existing = None
        try:
            existing = goto_find_user(email)
        except RuntimeError:
            existing = None
        if existing:
            _restore_goto_user(db, existing["user_key"], existing.get("email") or email)
            _touch_goto_profile(
                db,
                existing["user_key"],
                existing.get("email") or email,
                existing.get("first_name") or first_name,
                existing.get("last_name") or last_name,
                status=existing.get("status") or "",
                area=area,
            )
            local = db.get(GoToUserProfile, _local_user_key(email))
            if local:
                db.delete(local)
            user_payload = {**existing, **extra}
        else:
            _touch_goto_profile(
                db,
                _local_user_key(email),
                email,
                first_name,
                last_name,
                status="LOCAL",
                area=area,
            )
            user_payload = {
                "user_key": _local_user_key(email),
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "status": "LOCAL",
                "can_suspend": True,
                **extra,
            }
        _link_goto_user(db, user_payload.get("user_key") or "", email, first_name, last_name, body.machine_id, area)
        db.commit()
        machines = db.query(Machine).all()
        by_key, by_email = _phone_index(db)
        return _goto_user_out(user_payload, machines, by_key, by_email)
    try:
        existing = goto_find_user(email)
        if existing:
            if body.machine_id is not None:
                _link_goto_user(
                    db,
                    existing["user_key"],
                    existing.get("email") or email,
                    existing.get("first_name") or first_name,
                    existing.get("last_name") or last_name,
                    body.machine_id,
                    area,
                )
            _restore_goto_user(db, existing["user_key"], existing.get("email") or email)
            _touch_goto_profile(
                db,
                existing["user_key"],
                existing.get("email") or email,
                existing.get("first_name") or first_name,
                existing.get("last_name") or last_name,
                status=existing.get("status") or "",
                area=area,
            )
            local = db.get(GoToUserProfile, _local_user_key(email))
            if local:
                db.delete(local)
            try:
                existing = {**existing, **goto_reactivate_user(existing["user_key"])}
                extra = {**extra, "goto_warning": existing.get("goto_warning") or extra.get("goto_warning") or ""}
            except RuntimeError as exc:
                extra = {
                    **extra,
                    "goto_warning": (
                        f"Conta GoTo já existia, mas a licença não foi atribuída agora: {exc} "
                        "Tente Criar de novo ou atribua no portal."
                    ),
                }
            if body.phone.strip():
                _inherit_phone(
                    db,
                    existing["user_key"],
                    body.phone,
                    existing.get("first_name") or first_name,
                    existing.get("last_name") or last_name,
                    existing.get("email") or email,
                    wait_for_line=True,
                )
            _save_phone(
                db,
                existing["user_key"],
                existing.get("email") or email,
                body.phone,
                existing.get("first_name") or first_name,
                existing.get("last_name") or last_name,
            )
            db.commit()
            machines = db.query(Machine).all()
            by_key, by_email = _phone_index(db)
            return _goto_user_out({**existing, "existed": True, **extra}, machines, by_key, by_email)
        created = goto_create_user(email, first_name, last_name, body.send_welcome)
    except RuntimeError as exc:
        raise HTTPException(502, f"{exc}{already}") from exc
    extra = {**extra, "goto_warning": created.get("goto_warning") or extra.get("goto_warning") or ""}
    if not created.get("user_key"):
        raise HTTPException(502, f"GoTo não retornou a chave do usuário.{already}")
    _link_goto_user(
        db,
        created["user_key"],
        created.get("email") or email,
        first_name,
        last_name,
        body.machine_id,
        area,
    )
    _restore_goto_user(db, created["user_key"], created.get("email") or email)
    local = db.get(GoToUserProfile, _local_user_key(email))
    if local:
        db.delete(local)
    try:
        if body.phone.strip():
            _inherit_phone(
                db,
                created["user_key"],
                body.phone,
                first_name,
                last_name,
                created.get("email") or email,
                wait_for_line=True,
            )
    except RuntimeError as exc:
        raise HTTPException(502, f"{exc}{already}") from exc
    _save_phone(db, created["user_key"], created.get("email") or email, body.phone, first_name, last_name)
    db.commit()
    machines = db.query(Machine).all()
    by_key, by_email = _phone_index(db)
    return _goto_user_out({**created, **extra}, machines, by_key, by_email)


@app.put("/api/goto/users/{user_key}/link", response_model=GoToUserOut)
def link_goto_user(user_key: str, body: GoToUserLink, db: Session = Depends(get_db)):
    if not user_key.strip():
        raise HTTPException(400, "Usuário GoTo inválido")
    _link_goto_user(
        db,
        user_key.strip(),
        body.email,
        body.first_name,
        body.last_name,
        body.machine_id,
    )
    _restore_goto_user(db, user_key.strip(), body.email)
    db.commit()
    user = {
        "user_key": user_key.strip(),
        "email": body.email.strip(),
        "first_name": body.first_name.strip(),
        "last_name": body.last_name.strip(),
        "status": "",
        "can_suspend": True,
        "account_key": "",
    }
    machines = db.query(Machine).all()
    by_key, by_email = _phone_index(db)
    return _goto_user_out(user, machines, by_key, by_email)


@app.put("/api/goto/users/{user_key}/contact", response_model=GoToUserOut)
def update_goto_contact(user_key: str, body: GoToUserContact, db: Session = Depends(get_db)):
    try:
        phone = _inherit_phone(
            db,
            user_key.strip(),
            body.phone,
            body.first_name,
            body.last_name,
            body.email,
            wait_for_line=True,
        )
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    phone = _save_phone(
        db,
        user_key.strip(),
        body.email,
        phone,
        body.first_name,
        body.last_name,
    )
    db.commit()
    machines = db.query(Machine).all()
    by_key, by_email = _phone_index(db)
    return _goto_user_out(
        {
            "user_key": user_key.strip(),
            "email": body.email.strip(),
            "phone": phone,
        },
        machines,
        by_key,
        by_email,
    )


@app.post("/api/goto/users/{user_key}/suspend", response_model=GoToUserOut)
def suspend_goto_user(user_key: str, db: Session = Depends(get_db)):
    user_key = user_key.strip()
    if not user_key:
        raise HTTPException(400, "Usuário GoTo inválido")
    _first, _last, email = _goto_user_names(user_key, "")
    if _is_local_user_key(user_key):
        profile = db.get(GoToUserProfile, user_key)
        email = email or (profile.email if profile else user_key.split(":", 1)[-1])
        extra = _remove_syscall_user(email)
        _archive_goto_user(db, user_key, email)
        _release_user_machines(db, user_key, email)
        _exclude_goto_user(db, user_key, email)
        db.commit()
        machines = db.query(Machine).all()
        by_key, by_email = _phone_index(db)
        return _goto_user_out(
            {
                "user_key": user_key,
                "email": email,
                "first_name": (profile.first_name if profile else "") or "",
                "last_name": (profile.last_name if profile else "") or "",
                "status": "EXCLUIDO",
                **extra,
            },
            machines,
            by_key,
            by_email,
        )
    try:
        _archive_goto_user(db, user_key)
        _dismantle_goto_phone(db, user_key)
        suspended = goto_suspend_user(user_key)
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    extra = _remove_syscall_user(email or suspended.get("email") or "")
    _release_user_machines(db, user_key, email or suspended.get("email") or "")
    _exclude_goto_user(db, user_key, suspended.get("email") or email)
    db.commit()
    machines = db.query(Machine).all()
    by_key, by_email = _phone_index(db)
    return _goto_user_out({**suspended, **extra}, machines, by_key, by_email)


@app.get("/api/goto/history", response_model=list[GoToHistoryOut])
def list_goto_history(db: Session = Depends(get_db)):
    _backfill_history(db)
    rows = db.query(GoToUserHistory).order_by(GoToUserHistory.excluded_at.desc(), GoToUserHistory.id.desc()).all()
    return [
        GoToHistoryOut(
            id=row.id,
            user_key=row.user_key,
            email=row.email or "",
            first_name=row.first_name or "",
            last_name=row.last_name or "",
            phone=row.phone or "",
            area=row.area or "",
            hostname=row.hostname or "",
            bay_id=row.bay_id or "",
            entered_at=row.entered_at,
            excluded_at=row.excluded_at,
        )
        for row in rows
    ]


@app.get("/api/stats", response_model=StatsOut)
def stats(db: Session = Depends(get_db)):
    bays = db.query(Bay).options(joinedload(Bay.machines)).all()
    machines = db.query(Machine).options(joinedload(Machine.bay)).all()
    occupied = sum(1 for b in bays if b.machines)
    by_area: dict[str, int] = {}
    for m in machines:
        area = (m.area or "SEM ÁREA").upper()
        by_area[area] = by_area.get(area, 0) + 1
    by_zone: dict[str, dict[str, int]] = {}
    for b in bays:
        zone = by_zone.setdefault(b.zone, {"total": 0, "occupied": 0})
        zone["total"] += 1
        if b.machines:
            zone["occupied"] += 1
    unassigned = sum(1 for m in machines if not m.bay_id)
    return StatsOut(
        total_bays=len(bays),
        occupied=occupied,
        empty=len(bays) - occupied,
        machines=len(machines),
        unassigned=unassigned,
        by_area=by_area,
        by_zone=by_zone,
    )


def _csv_value(machine: Machine, attr: str) -> str:
    if attr == "bay_id":
        return machine.bay_id or ""
    value = getattr(machine, attr)
    if attr == "gpu":
        return "SIM" if value else "NAO"
    return "" if value is None else str(value)


@app.get("/api/export.csv")
def export_csv(db: Session = Depends(get_db)):
    machines = db.query(Machine).options(joinedload(Machine.bay)).order_by(Machine.hostname).all()
    buffer = StringIO()
    writer = csv.writer(buffer, delimiter=";")
    writer.writerow([header for header, _ in CSV_FIELDS])
    for machine in machines:
        writer.writerow([_csv_value(machine, attr) for _, attr in CSV_FIELDS])
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=red-layout-maquinas.csv"},
    )


@app.post("/api/import.csv")
async def import_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    raw = (await file.read()).decode("utf-8-sig")
    sample = raw[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
    except csv.Error:
        dialect = csv.excel
        dialect.delimiter = ";" if raw.count(";") >= raw.count(",") else ","
    reader = csv.DictReader(StringIO(raw), dialect=dialect)
    created = 0
    updated = 0
    header_map = {h.upper().strip(): attr for h, attr in CSV_FIELDS}
    for row in reader:
        data = {}
        for key, value in row.items():
            attr = header_map.get((key or "").upper().strip())
            if not attr:
                continue
            data[attr] = (value or "").strip()
        hostname = data.get("hostname")
        if not hostname:
            continue
        has_bay_column = "bay_id" in data
        if "gpu" in data:
            data["gpu"] = str(data["gpu"]).upper() in {"SIM", "TRUE", "1", "YES"}
        bay_id = data.pop("bay_id", "") or None
        machine = db.query(Machine).filter(Machine.hostname == hostname).first()
        if machine:
            for key, value in data.items():
                setattr(machine, key, value)
            updated += 1
        else:
            machine = Machine(**data)
            db.add(machine)
            db.flush()
            created += 1
        if has_bay_column:
            place_machine(db, machine, bay_id)
    db.commit()
    return {"created": created, "updated": updated}


app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")
