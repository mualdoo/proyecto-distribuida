"""
Tipos de mensaje y funciones para construirlos/parsearlos.
Cada mensaje es un dict serializado como JSON.

Estructura base:
{
    "msg_id":   str,   # UUID único — usado para deduplicación
    "type":     str,   # tipo del mensaje (ver MSG_* abajo)
    "node_id":  str,   # MAC del nodo que ORIGINÓ el mensaje
    "payload":  dict   # datos específicos del tipo
}
"""

import json
import uuid
from config import NODE_ID

# ── Tipos de mensaje ──────────────────────────────────────────────────────────
MSG_NODE_ANNOUNCE   = "NODE_ANNOUNCE"    # broadcast — nodo nuevo se anuncia
MSG_NODE_GOODBYE    = "NODE_GOODBYE"     # broadcast — nodo se va a desconectar
MSG_USER_REGISTERED = "USER_REGISTERED" # broadcast — nuevo usuario registrado
MSG_FILE_STORED     = "FILE_STORED"     # broadcast — archivo guardado/replicado
MSG_SPACE_QUERY     = "SPACE_QUERY"     # broadcast — pregunta cuánto espacio tiene cada nodo
MSG_SPACE_RESPONSE  = "SPACE_RESPONSE"  # punto a punto — respuesta con espacio disponible
MSG_FILE_TRANSFER   = "FILE_TRANSFER"   # punto a punto — envío de bytes de un PDF
MSG_SYNC_REQUEST  = "SYNC_REQUEST"
MSG_SYNC_RESPONSE = "SYNC_RESPONSE"
MSG_FILE_DELETED = "FILE_DELETED"


# ── Construcción ──────────────────────────────────────────────────────────────

def build(msg_type: str, payload: dict) -> str:
    """Construye un mensaje JSON listo para enviar."""
    return json.dumps({
        "msg_id":  str(uuid.uuid4()),
        "type":    msg_type,
        "node_id": NODE_ID,
        "payload": payload,
    })


def parse(raw: str) -> dict | None:
    """Parsea un mensaje JSON. Retorna None si es inválido."""
    try:
        msg = json.loads(raw)
        # Validar campos obligatorios
        if all(k in msg for k in ("msg_id", "type", "node_id", "payload")):
            return msg
        return None
    except (json.JSONDecodeError, TypeError):
        return None


# ── Helpers para construir payloads específicos ───────────────────────────────

def make_node_announce(ip: str, espacio: float) -> str:
    return build(MSG_NODE_ANNOUNCE, {"ip": ip, "espacio": espacio})

def make_node_goodbye() -> str:
    return build(MSG_NODE_GOODBYE, {})

def make_user_registered(nombre: str, contrasena_hash: str,
                          rol: str, intereses: str) -> str:
    return build(MSG_USER_REGISTERED, {
        "nombre":           nombre,
        "contrasena":       contrasena_hash,
        "rol":              rol,
        "intereses":        intereses,
    })

def make_file_stored(archivo_id: int, nombre: str, categoria: str,
                     subcategoria: str, confianza: float, hash_archivo: str,
                     propietario: str, nodo_primario: str,
                     nodo_replica: str) -> str:
    return build(MSG_FILE_STORED, {
        "archivo_id":    archivo_id,
        "nombre":        nombre,
        "categoria":     categoria,
        "subcategoria":  subcategoria,
        "confianza":     confianza,
        "hash_archivo":  hash_archivo,
        "propietario":   propietario,
        "nodo_primario": nodo_primario,
        "nodo_replica":  nodo_replica,
    })

def make_space_query(query_id: str) -> str:
    """query_id permite correlacionar respuestas con su pregunta original."""
    return build(MSG_SPACE_QUERY, {"query_id": query_id})

def make_space_response(query_id: str, espacio: float,
                        target_node_id: str) -> str:
    return build(MSG_SPACE_RESPONSE, {
        "query_id":      query_id,
        "espacio":       espacio,
        "target_node_id": target_node_id,  # a quién va dirigida la respuesta
    })

def make_sync_request(target_node_id: str) -> str:
    """Solicita sincronización a un nodo específico."""
    return build(MSG_SYNC_REQUEST, {"target_node_id": target_node_id})

def make_sync_response(target_node_id: str, payload: dict) -> str:
    return build(MSG_SYNC_RESPONSE, {
        "target_node_id": target_node_id,
        **payload
    })