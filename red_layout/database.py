from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from red_layout.models import Base

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "red_layout.db"

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False, "timeout": 30},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _ensure_columns() -> None:
    with engine.connect() as conn:
        tables = {row[0] for row in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))}
        if "machines" not in tables:
            return
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(machines)"))}
        if "ip" not in cols:
            conn.execute(text("ALTER TABLE machines ADD COLUMN ip VARCHAR(40) DEFAULT ''"))
        if "goto_user_key" not in cols:
            conn.execute(text("ALTER TABLE machines ADD COLUMN goto_user_key VARCHAR(80) DEFAULT ''"))
        if "bay_id" not in cols:
            conn.execute(text("ALTER TABLE machines ADD COLUMN bay_id VARCHAR(20)"))
            bay_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(bays)"))}
            if "machine_id" in bay_cols:
                conn.execute(text("""
                    UPDATE machines
                    SET bay_id = (
                        SELECT bays.id FROM bays WHERE bays.machine_id = machines.id
                    )
                    WHERE bay_id IS NULL OR bay_id = ''
                """))
        if "goto_user_profiles" in tables:
            profile_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(goto_user_profiles)"))}
            if "first_name" not in profile_cols:
                conn.execute(text("ALTER TABLE goto_user_profiles ADD COLUMN first_name VARCHAR(80) DEFAULT ''"))
            if "last_name" not in profile_cols:
                conn.execute(text("ALTER TABLE goto_user_profiles ADD COLUMN last_name VARCHAR(80) DEFAULT ''"))
            if "entered_at" not in profile_cols:
                conn.execute(text("ALTER TABLE goto_user_profiles ADD COLUMN entered_at DATETIME"))
            if "status" not in profile_cols:
                conn.execute(text("ALTER TABLE goto_user_profiles ADD COLUMN status VARCHAR(40) DEFAULT ''"))
            if "area" not in profile_cols:
                conn.execute(text("ALTER TABLE goto_user_profiles ADD COLUMN area VARCHAR(40) DEFAULT ''"))
        if "goto_user_history" in tables:
            history_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(goto_user_history)"))}
            if "area" not in history_cols:
                conn.execute(text("ALTER TABLE goto_user_history ADD COLUMN area VARCHAR(40) DEFAULT ''"))
        conn.commit()


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_columns()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
