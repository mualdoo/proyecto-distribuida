"""
Ejecutar una sola vez para crear el usuario admin inicial.
Uso: python create_admin.py
"""

from db.database import init_db
from db.models import MODELS, Usuario
from config import ROLE_ADMIN
from passlib.context import CryptContext

init_db(MODELS)

_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

ADMIN_NOMBRE    = "admin"
ADMIN_CONTRASENA = "admin123"  # cámbiala

def _preparar_contrasena(contrasena: str) -> str:
    return contrasena.encode("utf-8")[:72].decode("utf-8", errors="ignore")

if Usuario.select().where(Usuario.nombre == ADMIN_NOMBRE).exists():
    print(f"El usuario '{ADMIN_NOMBRE}' ya existe.")
else:
    Usuario.create(
        nombre=ADMIN_NOMBRE,
        contrasena=_pwd.hash(_preparar_contrasena(ADMIN_CONTRASENA)),
        rol=ROLE_ADMIN,
        intereses="",
    )
    print(f"Admin '{ADMIN_NOMBRE}' creado correctamente.")