"""
Escucha mensajes broadcast (SUB) de todos los nodos conocidos
y mensajes directos (ROUTER) dirigidos a este nodo.

Anti-bucle:
  - Cada mensaje tiene un msg_id único (UUID).
  - _seen_ids guarda los IDs ya procesados.
  - Mensajes propios (node_id == NODE_ID) se descartan — nosotros
    ya ejecutamos la acción antes de publicar, no al recibirla.
  - Nadie reenvía mensajes recibidos, solo reacciona localmente.
"""

import zmq
import threading
from collections import OrderedDict
from config import NODE_ID, ZMQ_BROADCAST_PORT, ZMQ_DIRECT_PORT
from messaging.protocol import parse
import messaging.handlers as handlers

# ── Caché de IDs vistos (máximo 1000 entradas) ───────────────────────────────
_seen_ids: OrderedDict[str, bool] = OrderedDict()
_SEEN_MAX = 1000


def _already_seen(msg_id: str) -> bool:
    if msg_id in _seen_ids:
        return True
    _seen_ids[msg_id] = True
    if len(_seen_ids) > _SEEN_MAX:
        _seen_ids.popitem(last=False)  # eliminar el más antiguo
    return False


# ── Dispatch ──────────────────────────────────────────────────────────────────

_HANDLERS = {
    "SYNC_REQUEST":  handlers.on_sync_request,
    "SYNC_RESPONSE": handlers.on_sync_response,
    "NODE_ANNOUNCE":   handlers.on_node_announce,
    "NODE_GOODBYE":    handlers.on_node_goodbye,
    "USER_REGISTERED": handlers.on_user_registered,
    "FILE_STORED":     handlers.on_file_stored,
    "SPACE_QUERY":     handlers.on_space_query,
    "SPACE_RESPONSE":  handlers.on_space_response,
    "FILE_DELETED": handlers.on_file_deleted,
}


def _dispatch(raw: str) -> None:
    msg = parse(raw)
    if msg is None:
        return

    # Descartar mensajes propios — ya actuamos antes de publicar
    if msg["node_id"] == NODE_ID:
        return

    # Descartar duplicados
    if _already_seen(msg["msg_id"]):
        return

    handler = _HANDLERS.get(msg["type"])
    if handler:
        handler(msg["payload"], msg["node_id"])


# ── Hilo SUB — escucha broadcasts ─────────────────────────────────────────────

def _sub_loop(context: zmq.Context, known_ips: list[str]) -> None:
    sock = context.socket(zmq.SUB)
    sock.setsockopt_string(zmq.SUBSCRIBE, "")  # recibir todo
    for ip in known_ips:
        sock.connect(f"tcp://{ip}:{ZMQ_BROADCAST_PORT}")
    while True:
        try:
            raw = sock.recv_string()
            _dispatch(raw)
        except zmq.ZMQError:
            break


# ── Hilo ROUTER — escucha mensajes directos ───────────────────────────────────

def _router_loop(context: zmq.Context) -> None:
    sock = context.socket(zmq.ROUTER)
    sock.bind(f"tcp://*:{ZMQ_DIRECT_PORT}")
    while True:
        try:
            # ROUTER recibe: [identity, empty, message]
            frames = sock.recv_multipart()
            if len(frames) >= 3:
                raw = frames[2].decode()
                _dispatch(raw)
        except zmq.ZMQError:
            break


# ── API pública ───────────────────────────────────────────────────────────────

def subscribe_to(ip: str, context: zmq.Context) -> None:
    """Conecta el SUB a un nuevo nodo descubierto en tiempo de ejecución."""
    # Se llama desde handlers.on_node_announce
    # Cada hilo SUB necesita reconectarse — solución simple: el nodo
    # guarda las IPs y reconecta al arrancar. Para conexiones dinámicas
    # se usa un socket SUB compartido accesible aquí.
    # Implementación completa en node.py donde el socket es accesible.
    pass


def start(context: zmq.Context, known_ips: list[str]) -> None:
    """Arranca los hilos de escucha en background."""
    threading.Thread(
        target=_sub_loop,
        args=(context, known_ips),
        daemon=True,
        name="zmq-sub"
    ).start()
    threading.Thread(
        target=_router_loop,
        args=(context,),
        daemon=True,
        name="zmq-router"
    ).start()