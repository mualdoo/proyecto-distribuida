"""
Descubrimiento automático de nodos vía UDP broadcast.

Flujo:
  1. Al arrancar, el nodo envía un broadcast UDP preguntando quién está en la red
  2. Todos los nodos activos que reciben la pregunta responden con su IP y NODE_ID
  3. El nodo nuevo conecta su SUB a las IPs recibidas
  4. A partir de ahí, ZeroMQ toma el control (NODE_ANNOUNCE, SYNC_REQUEST, etc.)

El hilo servidor corre siempre para responder a futuros nodos que se unan.
"""

import json
import socket
import threading
from config import (
    NODE_ID, NODE_IP,
    UDP_DISCOVERY_PORT,
    UDP_DISCOVERY_MSG,
    UDP_DISCOVERY_TIMEOUT,
)


# ── Servidor UDP — responde a descubrimientos ─────────────────────────────────

def _servidor_discovery() -> None:
    """
    Corre en background siempre.
    Cuando recibe un mensaje de descubrimiento, responde con este nodo.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", UDP_DISCOVERY_PORT))

    print(f"[discovery] Servidor UDP escuchando en puerto {UDP_DISCOVERY_PORT}")

    while True:
        try:
            data, addr = sock.recvfrom(1024)
            mensaje = data.decode("utf-8").strip()

            if mensaje == UDP_DISCOVERY_MSG:
                # No responderse a sí mismo
                if addr[0] == NODE_IP:
                    continue

                respuesta = json.dumps({
                    "node_id": NODE_ID,
                    "ip":      NODE_IP,
                })
                sock.sendto(respuesta.encode("utf-8"), addr)

        except Exception as e:
            print(f"[discovery] Error en servidor UDP: {e}")


# ── Cliente UDP — descubre nodos activos ──────────────────────────────────────

def descubrir_nodos() -> list[dict]:
    """
    Envía un broadcast UDP y espera respuestas durante UDP_DISCOVERY_TIMEOUT.
    Retorna lista de nodos descubiertos:
        [{"node_id": str, "ip": str}, ...]
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(UDP_DISCOVERY_TIMEOUT)

    nodos = []
    visto = set()

    try:
        sock.sendto(
            UDP_DISCOVERY_MSG.encode("utf-8"),
            ("<broadcast>", UDP_DISCOVERY_PORT)
        )

        while True:
            try:
                data, addr = sock.recvfrom(1024)
                info = json.loads(data.decode("utf-8"))

                node_id = info.get("node_id")
                ip      = info.get("ip")

                if node_id and ip and node_id not in visto and node_id != NODE_ID:
                    visto.add(node_id)
                    nodos.append({"node_id": node_id, "ip": ip})
                    print(f"[discovery] Nodo encontrado: {node_id} @ {ip}")

            except socket.timeout:
                break  # tiempo agotado, nadie más responde

    finally:
        sock.close()

    return nodos


# ── Arranque del servidor en background ───────────────────────────────────────

def start_servidor() -> None:
    threading.Thread(
        target=_servidor_discovery,
        daemon=True,
        name="udp-discovery"
    ).start()