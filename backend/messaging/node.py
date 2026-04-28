"""
Ciclo de vida del nodo ZeroMQ.
  - Al arrancar: anuncia presencia y se suscribe a los nodos conocidos
  - En ejecución: escucha mensajes en background
  - Al cerrar: anuncia despedida (graceful shutdown)
"""

import zmq
import atexit
import signal
import sys
from db.models import Nodo
from services.storage import obtener_espacio_disponible
from messaging.broadcaster import publish, close as close_pub
from messaging.listener import start as start_listeners
from messaging.protocol import make_node_announce, make_node_goodbye, make_sync_request
from config import NODE_ID, NODE_IP


def _get_known_ips() -> list[str]:
    """IPs de los nodos activos ya registrados en la DB local."""
    return [
        n.ip for n in Nodo.select().where(
            (Nodo.activo == True) & (Nodo.id != NODE_ID)
        )
    ]


def _on_shutdown(*_) -> None:
    """Envía NODE_GOODBYE y cierra sockets limpiamente."""
    print("[node] Enviando NODE_GOODBYE...")
    publish(make_node_goodbye())
    close_pub()
    sys.exit(0)


def start() -> None:
    context = zmq.Context.instance()
    known_ips = _get_known_ips()
    start_listeners(context, known_ips)

    # Pedir sincronización a un nodo activo antes de anunciarse
    # Así tenemos la DB actualizada antes de que la red nos conozca
    nodos_activos = list(Nodo.select().where(
        (Nodo.activo == True) & (Nodo.id != NODE_ID)
    ))
    if nodos_activos:
        import time
        nodo_sync = nodos_activos[0]
        publish(make_sync_request(nodo_sync.id))
        # Dar tiempo a recibir la respuesta antes de continuar
        time.sleep(2)

    # Ahora sí anunciarse con datos ya actualizados
    espacio = obtener_espacio_disponible()
    publish(make_node_announce(NODE_IP, espacio))

    atexit.register(_on_shutdown)
    signal.signal(signal.SIGTERM, _on_shutdown)
    signal.signal(signal.SIGINT,  _on_shutdown)

    print(f"[node] Nodo {NODE_ID} activo en {NODE_IP}")