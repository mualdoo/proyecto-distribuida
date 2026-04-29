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

    # 1. Arrancar servidor UDP para responder a futuros nodos
    start_servidor()

    # 2. Descubrir nodos activos en la red ahora mismo
    nodos_descubiertos = descubrir_nodos()

    # 3. Combinar con nodos conocidos en DB (por si es una reconexión)
    ips_db          = set(_get_known_ips())
    ips_descubiertas = {n["ip"] for n in nodos_descubiertos}
    todas_las_ips   = list(ips_db | ips_descubiertas)

    # 4. Registrar nodos descubiertos con su espacio real
    import httpx, asyncio

    def _fetch_espacio(ip: str) -> float:
        try:
            import httpx
            r = httpx.get(f"http://{ip}:8000/nodes/info", timeout=5)
            if r.status_code == 200:
                return r.json().get("espacio_disponible", 0)
        except Exception:
            pass
        return 0

    for nodo in nodos_descubiertos:
        espacio = _fetch_espacio(nodo["ip"])
        Nodo.insert(
            id=nodo["node_id"],
            ip=nodo["ip"],
            espacio_disponible=espacio,
            activo=True,
            ultima_vez_visto=datetime.now(timezone.utc),
        ).on_conflict(
            conflict_target=[Nodo.id],
            update={
                Nodo.ip:                 nodo["ip"],
                Nodo.espacio_disponible: espacio,
                Nodo.activo:             True,
                Nodo.ultima_vez_visto:   datetime.now(timezone.utc),
            }
        ).execute()

    # 5. Registrarse a sí mismo en la DB local
    Nodo.insert(
        id=NODE_ID,
        ip=NODE_IP,
        espacio_disponible=obtener_espacio_disponible(),
        activo=True,
        ultima_vez_visto=datetime.now(timezone.utc),
    ).on_conflict(
        conflict_target=[Nodo.id],
        update={
            Nodo.ip:                 NODE_IP,
            Nodo.espacio_disponible: obtener_espacio_disponible(),
            Nodo.activo:             True,
            Nodo.ultima_vez_visto:   datetime.now(timezone.utc),
        }
    ).execute()

    # 6. Arrancar listeners con todas las IPs conocidas
    start_listeners(context, todas_las_ips)

    # 7. Pedir sincronización a un nodo activo si los hay
    if nodos_descubiertos:
        import time
        nodo_sync = nodos_descubiertos[0]
        publish(make_sync_request(nodo_sync["node_id"]))
        time.sleep(2)

    # 8. Anunciarse a la red
    publish(make_node_announce(NODE_IP, obtener_espacio_disponible()))

    atexit.register(_on_shutdown)
    signal.signal(signal.SIGTERM, _on_shutdown)
    signal.signal(signal.SIGINT,  _on_shutdown)

    print(f"[node] Nodo {NODE_ID} activo en {NODE_IP}")