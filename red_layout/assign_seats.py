from red_layout.database import SessionLocal, init_db
from red_layout.models import Bay, Machine
from red_layout.sync_assets import match_person

SEATS = [
    ("C-01", "Clauber"),
    ("C-02", "Juliana"),
    ("C-04", "Fernanda Costa"),
    ("C-05", "Julia"),
    ("C-06", "Karina"),
    ("C-08", "Cirlene"),
    ("C-09", "Evely"),
    ("C-10", "Jessica"),
    ("C-15", "Amanda Cris"),
    ("C-22", "Telma"),
    ("C-23", "Wesley"),
    ("C-24", "Christopher"),
    ("C-21", "Aline"),
    ("J-01", "Jose"),
    ("J-02", "Amanda Volani"),
    ("J-04", "Camila Cuelhar"),
    ("J-07", "Marina"),
    ("J-08", "Lucas"),
    ("J-11", "Renata"),
    ("J-12", "Ana Locatelli"),
]

NEW_PEOPLE = [
    {
        "hostname": "AMANDA-VOLANI",
        "user_name": "Amanda Volani",
        "area": "JURIDICO",
        "email": "amanda.volani@redprecatorios.com.br",
        "numero_ind": "551135666241",
    },
    {
        "hostname": "MARINA",
        "user_name": "Marina",
        "area": "JURIDICO",
    },
]


def unassign(db, machine_id: int) -> None:
    machine = db.get(Machine, machine_id)
    if machine:
        machine.bay_id = None


def ensure_person(db, data: dict) -> Machine:
    machine = db.query(Machine).filter(Machine.user_name == data["user_name"]).first()
    if machine:
        return machine
    machine = db.query(Machine).filter(Machine.hostname == data["hostname"]).first()
    if machine:
        machine.user_name = data["user_name"]
        return machine
    machine = Machine(**data)
    db.add(machine)
    db.flush()
    return machine


def run() -> None:
    init_db()
    db = SessionLocal()
    try:
        for data in NEW_PEOPLE:
            ensure_person(db, data)
        db.flush()

        people = [m for m in db.query(Machine).all() if m.user_name]
        missing = []
        assigned = []
        for bay_id, name in SEATS:
            bay = db.get(Bay, bay_id)
            if not bay:
                missing.append(f"{name} → baia {bay_id} não existe")
                continue
            machine = match_person(name, people)
            if not machine:
                missing.append(f"{name} → sem máquina")
                continue
            unassign(db, machine.id)
            for other in db.query(Machine).filter(Machine.bay_id == bay_id, Machine.id != machine.id).all():
                other.bay_id = None
            machine.bay_id = bay.id
            assigned.append(f"{bay_id} ← {machine.user_name} ({machine.hostname})")
        db.commit()
        print("Alocado:")
        for line in assigned:
            print(" ", line)
        if missing:
            print("Falhou:")
            for line in missing:
                print(" ", line)
    finally:
        db.close()


if __name__ == "__main__":
    run()
