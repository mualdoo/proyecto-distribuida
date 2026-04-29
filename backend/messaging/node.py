import zmq
import atexit
import signal
import sys
from datetime import datetime, timezone

from backend.db.models import Nodo
from backend.services.storage import obtener_espacio_disponible
from backend.messaging.broadcaster import publish, close as close_pub
from backend.messaging.listener import start as start_listeners
from backend.messaging.discovery import descubrir_nodos, start_servidor
from backend.messaging.protocol import make_node_announce, make_node_goodbye, make_sync_request
from backend.config import NODE_ID, NODE_IP, BASE_DIR


def _get_known_ips() -> list[str]:
    """IPs de nodos activos ya en la DB local (reconexiones)."""
    return [
        n.ip for n in Nodo.select().where(
            (Nodo.activo == True) & (Nodo.id != NODE_ID)
        )
    ]


def _on_shutdown(*_) -> None:
    print("[node] Enviando NODE_GOODBYE...")
    publish(make_node_goodbye())
    close_pub()
    sys.exit(0)


def start() -> None:
    context = zmq.Context.instance()
    known_ips = _get_known_ips()
    start_listeners(context, known_ips)

    # ── Registrarse en la DB local ────────────────────────────────────────────
    from backend.db.models import Nodo
    from datetime import datetime, timezone
    Nodo.insert(
        id=NODE_ID,
        ip=NODE_IP,
        espacio_disponible=obtener_espacio_disponible(),
        activo=True,
        ultima_vez_visto=datetime.now(timezone.utc),
    ).on_conflict(
        conflict_target=[Nodo.id],
        update={
            Nodo.ip: NODE_IP,
            Nodo.espacio_disponible: obtener_espacio_disponible(),
            Nodo.activo: True,
            Nodo.ultima_vez_visto: datetime.now(timezone.utc),
        }
    ).execute()

    # Sincronización y anuncio (igual que antes)
    nodos_activos = list(Nodo.select().where(
        (Nodo.activo == True) & (Nodo.id != NODE_ID)
    ))
    if nodos_activos:
        import time
        nodo_sync = nodos_activos[0]
        publish(make_sync_request(nodo_sync.id))
        time.sleep(2)

    espacio = obtener_espacio_disponible()
    publish(make_node_announce(NODE_IP, espacio))

    atexit.register(_on_shutdown)
    signal.signal(signal.SIGTERM, _on_shutdown)
    signal.signal(signal.SIGINT,  _on_shutdown)

    print(f"[node] Nodo {NODE_ID} activo en {NODE_IP}")