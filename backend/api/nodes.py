"""
Endpoints para consultar el estado de los nodos (visible para admin).
"""

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from backend.db.models import Nodo
from backend.api.auth import require_admin
from backend.services.storage import obtener_espacio_disponible
from backend.config import NODE_ID, NODE_IP
from backend.messaging.listener import subscribe_to
from backend.messaging.broadcaster import publish
from backend.messaging.protocol import make_node_announce


router = APIRouter(prefix="/nodes", tags=["nodes"])


class NodoInfoSchema(BaseModel):
    node_id: str
    ip: str

@router.post("/introduce")
def introduce(datos: NodoInfoSchema):
    """
    Un nodo nuevo nos avisa de su existencia directamente.
    Nos suscribimos a él y le respondemos con nuestro NODE_ANNOUNCE
    para que nos agregue a su DB.
    """
    from db.models import Nodo
    from datetime import datetime, timezone

    # Registrarlo en nuestra DB
    Nodo.insert(
        id=datos.node_id,
        ip=datos.ip,
        espacio_disponible=0,
        activo=True,
        ultima_vez_visto=datetime.now(timezone.utc),
    ).on_conflict(
        conflict_target=[Nodo.id],
        update={
            Nodo.ip:     datos.ip,
            Nodo.activo: True,
            Nodo.ultima_vez_visto: datetime.now(timezone.utc),
        }
    ).execute()

    # Suscribirse a sus mensajes ZeroMQ
    subscribe_to(datos.ip)

    # Responderle con nuestro anuncio para que él también nos registre
    publish(make_node_announce(NODE_IP, obtener_espacio_disponible()))

    return {"ok": True}


@router.get("/")
def listar_nodos(admin=Depends(require_admin)):
    return [
        {
            "id":                  n.id,
            "ip":                  n.ip,
            "espacio_disponible":  n.espacio_disponible,
            "activo":              n.activo,
            "ultima_vez_visto":    n.ultima_vez_visto.isoformat(),
        }
        for n in Nodo.select().order_by(Nodo.activo.desc())
    ]


@router.get("/info")
def info_nodo():
    """Endpoint público — no requiere auth — para que otros nodos lean el espacio."""
    return {
        "node_id":            NODE_ID,
        "ip":                 NODE_IP,
        "espacio_disponible": obtener_espacio_disponible(),
    }