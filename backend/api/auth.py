"""
Endpoints de autenticación y sesiones.
Sesiones server-side con itsdangerous — el token se guarda en cookie.
"""

from fastapi import APIRouter, HTTPException, Response, Request, Depends
from pydantic import BaseModel
from passlib.context import CryptContext
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from backend.db.models import Usuario
from backend.messaging.broadcaster import publish
from backend.messaging.protocol import make_user_registered
from backend.config import SECRET_KEY, SESSION_MAX_AGE, ROLE_USER, ROLE_ADMIN

router = APIRouter(prefix="/auth", tags=["auth"])

_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")
_serializer = URLSafeTimedSerializer(SECRET_KEY)


# ── Utilidades de sesión ──────────────────────────────────────────────────────

def crear_sesion(response: Response, usuario: Usuario) -> None:
    token = _serializer.dumps({"nombre": usuario.nombre, "rol": usuario.rol})
    response.set_cookie(
        key="session",
        value=token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
    )


def get_sesion(request: Request) -> dict:
    """Dependency — retorna los datos de sesión o lanza 401."""
    token = request.cookies.get("session")
    if not token:
        raise HTTPException(status_code=401, detail="No autenticado.")
    try:
        return _serializer.loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        raise HTTPException(status_code=401, detail="Sesión inválida o expirada.")


def get_usuario_actual(sesion: dict = Depends(get_sesion)) -> Usuario:
    """Dependency — retorna el objeto Usuario de la sesión activa."""
    try:
        return Usuario.get(Usuario.nombre == sesion["nombre"])
    except Usuario.DoesNotExist:
        raise HTTPException(status_code=401, detail="Usuario no encontrado.")


def require_admin(usuario: Usuario = Depends(get_usuario_actual)) -> Usuario:
    """Dependency — verifica que el usuario sea admin."""
    if usuario.rol != ROLE_ADMIN:
        raise HTTPException(status_code=403, detail="Se requiere rol admin.")
    return usuario


# ── Schemas ───────────────────────────────────────────────────────────────────

class RegistroSchema(BaseModel):
    nombre:    str
    contrasena: str
    intereses: list[str]  # categorías elegidas por el usuario


class LoginSchema(BaseModel):
    nombre:    str
    contrasena: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/registro", status_code=201)
def registro(datos: RegistroSchema, response: Response):
    if Usuario.select().where(Usuario.nombre == datos.nombre).exists():
        raise HTTPException(status_code=409, detail="El nombre de usuario ya existe.")

    hash_pw = _pwd.hash(datos.contrasena)
    intereses_str = ",".join(datos.intereses)

    usuario = Usuario.create(
        nombre=datos.nombre,
        contrasena=hash_pw,
        rol=ROLE_USER,
        intereses=intereses_str,
    )

    # Anunciar a todos los nodos
    publish(make_user_registered(
        nombre=datos.nombre,
        contrasena_hash=hash_pw,
        rol=ROLE_USER,
        intereses=intereses_str,
    ))

    crear_sesion(response, usuario)
    return {"mensaje": "Usuario registrado correctamente."}


@router.post("/login")
def login(datos: LoginSchema, response: Response):
    try:
        usuario = Usuario.get(Usuario.nombre == datos.nombre)
    except Usuario.DoesNotExist:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas.")

    if not _pwd.verify(datos.contrasena, usuario.contrasena):
        raise HTTPException(status_code=401, detail="Credenciales incorrectas.")

    crear_sesion(response, usuario)
    return {"mensaje": "Login exitoso.", "rol": usuario.rol}


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie("session")
    return {"mensaje": "Sesión cerrada."}


@router.get("/me")
def me(usuario: Usuario = Depends(get_usuario_actual)):
    return {
        "nombre":    usuario.nombre,
        "rol":       usuario.rol,
        "intereses": usuario.intereses.split(","),
    }