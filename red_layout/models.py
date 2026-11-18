from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Machine(Base):
    __tablename__ = "machines"

    id: Mapped[int] = mapped_column(primary_key=True)
    hostname: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    ip: Mapped[str] = mapped_column(String(40), default="")
    observacao: Mapped[str] = mapped_column(Text, default="")
    gpu: Mapped[bool] = mapped_column(Boolean, default=False)
    script: Mapped[str] = mapped_column(String(120), default="")
    user_name: Mapped[str] = mapped_column(String(160), default="")
    area: Mapped[str] = mapped_column(String(80), default="")
    email: Mapped[str] = mapped_column(String(200), default="")
    ramal: Mapped[str] = mapped_column(String(40), default="")
    numero_ind: Mapped[str] = mapped_column(String(40), default="")
    win_product_key: Mapped[str] = mapped_column(String(80), default="")
    win_product_id: Mapped[str] = mapped_column(String(80), default="")
    win_versao: Mapped[str] = mapped_column(String(40), default="")
    win_status: Mapped[str] = mapped_column(String(40), default="ATIVO")
    office_key: Mapped[str] = mapped_column(String(80), default="")
    office_status: Mapped[str] = mapped_column(String(40), default="PENDENTE")
    ultima_atualizacao: Mapped[str] = mapped_column(String(40), default="")
    politica: Mapped[str] = mapped_column(String(80), default="")
    bitlocker_status: Mapped[str] = mapped_column(String(40), default="")
    goto_user_key: Mapped[str] = mapped_column(String(80), default="")
    bay_id: Mapped[str | None] = mapped_column(ForeignKey("bays.id"), index=True, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    bay: Mapped["Bay | None"] = relationship(back_populates="machines")


class GoToUserProfile(Base):
    __tablename__ = "goto_user_profiles"

    user_key: Mapped[str] = mapped_column(String(80), primary_key=True)
    email: Mapped[str] = mapped_column(String(200), default="")
    first_name: Mapped[str] = mapped_column(String(80), default="")
    last_name: Mapped[str] = mapped_column(String(80), default="")
    phone: Mapped[str] = mapped_column(String(40), default="")
    status: Mapped[str] = mapped_column(String(40), default="")
    area: Mapped[str] = mapped_column(String(40), default="")
    entered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class GoToPhoneNumber(Base):
    __tablename__ = "goto_phone_numbers"

    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    number: Mapped[str] = mapped_column(String(40), index=True)
    name: Mapped[str] = mapped_column(String(160), default="")
    status: Mapped[str] = mapped_column(String(40), default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class GoToExcludedUser(Base):
    __tablename__ = "goto_excluded_users"

    user_key: Mapped[str] = mapped_column(String(80), primary_key=True)
    email: Mapped[str] = mapped_column(String(200), default="")
    excluded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class GoToUserHistory(Base):
    __tablename__ = "goto_user_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_key: Mapped[str] = mapped_column(String(80), index=True)
    email: Mapped[str] = mapped_column(String(200), default="")
    first_name: Mapped[str] = mapped_column(String(80), default="")
    last_name: Mapped[str] = mapped_column(String(80), default="")
    phone: Mapped[str] = mapped_column(String(40), default="")
    area: Mapped[str] = mapped_column(String(40), default="")
    hostname: Mapped[str] = mapped_column(String(80), default="")
    bay_id: Mapped[str] = mapped_column(String(20), default="")
    entered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    excluded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Bay(Base):
    __tablename__ = "bays"

    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    zone: Mapped[str] = mapped_column(String(40))
    label: Mapped[str] = mapped_column(String(80))
    notes: Mapped[str] = mapped_column(Text, default="")

    machines: Mapped[list[Machine]] = relationship(
        back_populates="bay",
        order_by="Machine.hostname",
    )
