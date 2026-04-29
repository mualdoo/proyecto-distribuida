"""
Endpoints para consultar el estado de los nodos (visible para admin).
"""

from fastapi import APIRouter, Depends

from backend.db.models import Nodo
from backend.api.auth import require_admin
from backend.services.storage import obtener_espacio_disponible
from backend.config import NODE_ID, NODE_IP

router = APIRouter(prefix="/nodes", tags=["nodes"])


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