"""
Endpoints de administración — solo accesibles con rol admin.
Gestión de cuentas y archivos de cualquier usuario.
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from passlib.context import CryptContext

from db.models import Usuario, Archivo
from api.auth import require_admin
from api.files import _eliminar_archivo_completo
from config import ROLE_USER, ROLE_ADMIN
from messaging.broadcaster import publish
from messaging.protocol import make_user_registered

router = APIRouter(prefix="/admin", tags=["admin"])
_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ── Usuarios ──────────────────────────────────────────────────────────────────

@router.get("/usuarios")
def listar_usuarios(admin=Depends(require_admin)):
    return [
        {
            "nombre":    u.nombre,
            "rol":       u.rol,
            "intereses": u.intereses.split(","),
        }
        for u in Usuario.select()
    ]


class CrearUsuarioSchema(BaseModel):
    nombre:    str
    contrasena: str
    rol:       str = ROLE_USER
    intereses: list[str] = []


@router.post("/usuarios", status_code=201)
def crear_usuario(datos: CrearUsuarioSchema, admin=Depends(require_admin)):
    if Usuario.select().where(Usuario.nombre == datos.nombre).exists():
        raise HTTPException(status_code=409, detail="El usuario ya existe.")

    if datos.rol not in (ROLE_USER, ROLE_ADMIN):
        raise HTTPException(status_code=400, detail="Rol inválido.")

    hash_pw = _pwd.hash(datos.contrasena)
    intereses_str = ",".join(datos.intereses)

    Usuario.create(
        nombre=datos.nombre,
        contrasena=hash_pw,
        rol=datos.rol,
        intereses=intereses_str,
    )

    publish(make_user_registered(
        nombre=datos.nombre,
        contrasena_hash=hash_pw,
        rol=datos.rol,
        intereses=intereses_str,
    ))

    return {"mensaje": f"Usuario '{datos.nombre}' creado."}


@router.delete("/usuarios/{nombre}")
def eliminar_usuario(nombre: str, admin=Depends(require_admin)):
    try:
        usuario = Usuario.get(Usuario.nombre == nombre)
    except Usuario.DoesNotExist:
        raise HTTPException(status_code=404, detail="Usuario no encontrado.")

    if usuario.rol == ROLE_ADMIN:
        raise HTTPException(status_code=400, detail="No se puede eliminar a otro admin.")

    # Eliminar todos sus archivos primero
    for archivo in usuario.archivos:
        _eliminar_archivo_completo(archivo, usuario.nombre)

    usuario.delete_instance(recursive=True)
    return {"mensaje": f"Usuario '{nombre}' eliminado."}


# ── Archivos ──────────────────────────────────────────────────────────────────

@router.get("/archivos")
def listar_todos_los_archivos(
    usuario_nombre: str | None = None,
    admin=Depends(require_admin),
):
    query = Archivo.select()
    if usuario_nombre:
        try:
            u = Usuario.get(Usuario.nombre == usuario_nombre)
            query = query.where(Archivo.propietario == u.id)
        except Usuario.DoesNotExist:
            raise HTTPException(status_code=404, detail="Usuario no encontrado.")

    return [
        {
            "id":           a.id,
            "nombre":       a.nombre,
            "categoria":    a.categoria,
            "subcategoria": a.subcategoria,
            "propietario":  a.propietario.nombre,
            "fecha_subida": a.fecha_subida.isoformat(),
            "ubicaciones": [
                {"nodo_id": u.nodo_id, "es_replica": u.es_replica}
                for u in a.ubicaciones
            ],
        }
        for a in query
    ]


@router.delete("/archivos/{archivo_id}")
def eliminar_archivo_admin(archivo_id: int, admin=Depends(require_admin)):
    try:
        archivo = Archivo.get(Archivo.id == archivo_id)
    except Archivo.DoesNotExist:
        raise HTTPException(status_code=404, detail="Archivo no encontrado.")

    _eliminar_archivo_completo(archivo, archivo.propietario.nombre)
    return {"mensaje": "Archivo eliminado."}