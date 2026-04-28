"""
Publica mensajes broadcast vía ZeroMQ PUB.
Un solo socket PUB por nodo — todos los demás escuchan con SUB.
"""

import zmq
from config import ZMQ_BROADCAST_PORT

_context  = zmq.Context.instance()
_pub_sock = _context.socket(zmq.PUB)
_pub_sock.bind(f"tcp://*:{ZMQ_BROADCAST_PORT}")


def publish(message: str) -> None:
    """Envía un mensaje a todos los nodos suscritos."""
    _pub_sock.send_string(message)


def close() -> None:
    _pub_sock.close()