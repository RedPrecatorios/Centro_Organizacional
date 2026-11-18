from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class MachineBase(BaseModel):
    hostname: str = Field(min_length=1, max_length=80)
    ip: str = ""
    observacao: str = ""
    gpu: bool = False
    script: str = ""
    user_name: str = ""
    area: str = ""
    email: str = ""
    ramal: str = ""
    numero_ind: str = ""
    win_product_key: str = ""
    win_product_id: str = ""
    win_versao: str = ""
    win_status: str = "ATIVO"
    office_key: str = ""
    office_status: str = "PENDENTE"
    ultima_atualizacao: str = ""
    politica: str = ""
    bitlocker_status: str = ""
    goto_user_key: str = ""


class MachineCreate(MachineBase):
    bay_id: str | None = None


class MachineUpdate(BaseModel):
    hostname: str | None = Field(default=None, min_length=1, max_length=80)
    ip: str | None = None
    observacao: str | None = None
    gpu: bool | None = None
    script: str | None = None
    user_name: str | None = None
    area: str | None = None
    email: str | None = None
    ramal: str | None = None
    numero_ind: str | None = None
    win_product_key: str | None = None
    win_product_id: str | None = None
    win_versao: str | None = None
    win_status: str | None = None
    office_key: str | None = None
    office_status: str | None = None
    ultima_atualizacao: str | None = None
    politica: str | None = None
    bitlocker_status: str | None = None
    goto_user_key: str | None = None
    bay_id: str | None = None


class MachineOut(MachineBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    bay_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class BayOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    zone: str
    label: str
    notes: str = ""
    machines: list[MachineOut] = Field(default_factory=list)
    machine: MachineOut | None = None


class BayNotesUpdate(BaseModel):
    notes: str = ""


class AssignBody(BaseModel):
    machine_id: int | None = None


class StatsOut(BaseModel):
    total_bays: int
    occupied: int
    empty: int
    machines: int
    unassigned: int
    by_area: dict[str, int]
    by_zone: dict[str, dict[str, int]]


class GoToUserCreate(BaseModel):
    first_name: str = Field(min_length=1, max_length=80)
    last_name: str = Field(min_length=1, max_length=80)
    email: str = Field(min_length=3, max_length=200)
    area: str = Field(min_length=1, max_length=40)
    phone: str = ""
    send_welcome: bool = True
    machine_id: int | None = None
    enable_goto: bool = False


class GoToUserLink(BaseModel):
    machine_id: int | None = None
    email: str = ""
    first_name: str = ""
    last_name: str = ""


class GoToUserContact(BaseModel):
    phone: str = ""
    email: str = ""
    first_name: str = ""
    last_name: str = ""


class GoToStatusOut(BaseModel):
    ok: bool
    email: str = ""
    account_key: str = ""
    account_name: str = ""


class MondayStatusOut(BaseModel):
    ok: bool
    email: str = ""
    name: str = ""
    board_id: str = ""
    board_name: str = ""


class PlataformaStatusOut(BaseModel):
    ok: bool
    host: str = ""
    database: str = ""
    users: int = 0


class SyscallStatusOut(BaseModel):
    ok: bool
    url: str = ""
    users: int = 0


class GoToUserOut(BaseModel):
    user_key: str
    email: str
    first_name: str = ""
    last_name: str = ""
    status: str = ""
    can_suspend: bool = False
    account_key: str = ""
    machine_id: int | None = None
    hostname: str = ""
    bay_id: str | None = None
    user_name: str = ""
    phone: str = ""
    existed: bool = False
    monday_invited: bool = False
    monday_existed: bool = False
    monday_team: str = ""
    monday_column: str = ""
    monday_label: str = ""
    monday_label_created: bool = False
    monday_warning: str = ""
    plataforma_created: bool = False
    plataforma_existed: bool = False
    plataforma_username: str = ""
    plataforma_password: str = ""
    plataforma_warning: str = ""
    syscall_created: bool = False
    syscall_existed: bool = False
    syscall_skipped: bool = False
    syscall_username: str = ""
    syscall_password: str = ""
    syscall_deleted: bool = False
    syscall_warning: str = ""
    goto_warning: str = ""


class GoToUserLookupOut(BaseModel):
    found: bool
    user: GoToUserOut | None = None


class GoToPhoneOut(BaseModel):
    id: str
    number: str
    name: str = ""
    status: str = ""
    label: str = ""
    assigned_to: str = ""
    assigned_user_key: str = ""


class GoToPhoneListOut(BaseModel):
    count: int = 0
    phones: list[GoToPhoneOut]


class GoToHistoryOut(BaseModel):
    id: int
    user_key: str
    email: str = ""
    first_name: str = ""
    last_name: str = ""
    phone: str = ""
    area: str = ""
    hostname: str = ""
    bay_id: str = ""
    entered_at: datetime | None = None
    excluded_at: datetime | None = None
