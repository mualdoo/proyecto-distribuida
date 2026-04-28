"""
Endpoints para consultar el estado de los nodos (visible para admin).
"""

from fastapi import APIRouter, Depends
from db.models import Nodo
from api.auth import require_admin

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