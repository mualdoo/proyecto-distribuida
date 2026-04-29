from peewee import (
    Model, CharField, FloatField, TextField,
    ForeignKeyField, DateTimeField, BooleanField
)
from datetime import datetime, timezone

from backend.db.database import db
from backend.config import ROLE_USER


class BaseModel(Model):
    class Meta:
        database = db


class Nodo(BaseModel):
    id               = CharField(primary_key=True)   # MAC address
    ip               = CharField()
    espacio_disponible = FloatField()                 # bytes libres
    activo           = BooleanField(default=True)
    ultima_vez_visto = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "nodo"


class Usuario(BaseModel):
    nombre     = CharField(unique=True)
    contrasena = CharField()                          # hash bcrypt
    rol        = CharField(default=ROLE_USER)
    intereses  = TextField(default="")               # categorías separadas por coma

    class Meta:
        table_name = "usuario"


class Archivo(BaseModel):
    nombre       = CharField()
    categoria    = CharField()
    subcategoria = CharField()
    confianza    = FloatField()                       # 0.0 – 1.0
    hash_archivo = CharField(unique=True)             # SHA-256 por archivo por usuario
    fecha_subida = DateTimeField(default=lambda: datetime.now(timezone.utc))
    propietario  = ForeignKeyField(Usuario, backref="archivos", on_delete="CASCADE")

    class Meta:
        table_name = "archivo"


class UbicacionArchivo(BaseModel):
    nodo    = ForeignKeyField(Nodo, backref="ubicaciones", on_delete="CASCADE")
    archivo = ForeignKeyField(Archivo, backref="ubicaciones", on_delete="CASCADE")
    es_replica = BooleanField(default=False)         # False = original, True = réplica

    class Meta:
        table_name = "ubicacion_archivo"
        indexes = (
            (("nodo", "archivo"), True),             # un nodo no repite el mismo archivo
        )


# Lista para pasar a init_db()
MODELS = [Nodo, Usuario, Archivo, UbicacionArchivo]