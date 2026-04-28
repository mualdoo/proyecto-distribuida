from peewee import SqliteDatabase
from config import DATABASE_PATH

DATABASE_PATH.parent.mkdir(exist_ok=True)

db = SqliteDatabase(
    str(DATABASE_PATH),
    pragmas={
        "journal_mode": "wal",   # permite lecturas concurrentes mientras se escribe
        "foreign_keys": 1,
    }
)

def init_db(models: list) -> None:
    """Crea las tablas si no existen. Llamar al arrancar la app."""
    with db:
        db.create_tables(models, safe=True)