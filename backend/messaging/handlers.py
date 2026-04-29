"""
Reacciona a los mensajes recibidos actualizando la DB local.

Regla de oro: los handlers NUNCA publican mensajes.
Solo leen/escriben en la DB local. Publicar es responsabilidad
exclusiva del nodo que origina la acción (en la API o en node.py).
Esto elimina cualquier posibilidad de bucle.
"""

import asyncio
from datetime import datetime, timezone

from backend.db.models import Nodo, Usuario, Archivo, UbicacionArchivo
from backend.services.storage import obtener_espacio_disponible
from backend.messaging.protocol import make_space_response, make_sync_response
from backend.config import NODE_ID, ZMQ_DIRECT_PORT

# Registry de respuestas SPACE_RESPONSE pendientes
# query_id -> list de respuestas recibidas
# Lo usa replication.py para esperar las respuestas
_space_responses: dict[str, list[dict]] = {}
_space_events:    dict[str, asyncio.Event] = {}


# ── NODE_ANNOUNCE ─────────────────────────────────────────────────────────────

def on_node_announce(payload: dict, sender_node_id: str) -> None:
    print(f"[handler] NODE_ANNOUNCE recibido de {sender_node_id}: {payload}")
    Nodo.insert(
        id=sender_node_id,
        ip=payload["ip"],
        espacio_disponible=payload["espacio"],
        activo=True,
        ultima_vez_visto=datetime.now(timezone.utc),
    ).on_conflict(
        conflict_target=[Nodo.id],
        update={
            Nodo.ip: payload["ip"],
            Nodo.espacio_disponible: payload["espacio"],
            Nodo.activo: True,
            Nodo.ultima_vez_visto: datetime.now(timezone.utc),
        }
    ).execute()

    # Suscribirse al nuevo nodo para recibir sus futuros mensajes
    from backend.messaging.listener import subscribe_to
    subscribe_to(payload["ip"])


# ── NODE_GOODBYE ──────────────────────────────────────────────────────────────

def _soy_responsable_de_rereplica() -> bool:
    """
    Solo el nodo activo con el ID lexicográficamente mayor
    se encarga de la re-replicación.
    Todos los nodos llegan a la misma conclusión de forma independiente.
    """
    nodos_activos = [
        n.id for n in Nodo.select().where(Nodo.activo == True)
    ]
    if not nodos_activos:
        return False
    return max(nodos_activos) == NODE_ID


def on_node_goodbye(payload: dict, sender_node_id: str) -> None:
    Nodo.update(activo=False).where(Nodo.id == sender_node_id).execute()

    # Solo un nodo actúa, los demás ignoran
    if not _soy_responsable_de_rereplica():
        return

    from backend.services.replication import obtener_archivos_sin_replica
    import threading
    huerfanos = obtener_archivos_sin_replica(sender_node_id)

    if huerfanos:
        threading.Thread(
            target=_rereplicate_sync,
            args=(huerfanos,),
            daemon=True
        ).start()


def _rereplicate_sync(huerfanos: list) -> None:
    """Wrapper síncrono para llamar código async desde un hilo normal."""
    import asyncio
    from backend.services.replication import rereplicate_archivo
    from backend.messaging.broadcaster import publish
    from backend.messaging.protocol import make_space_query
    import uuid

    query_id = str(uuid.uuid4())
    publish(make_space_query(query_id))

    # Esperar respuestas brevemente
    import time
    from backend.config import SPACE_QUERY_TIMEOUT_MS
    time.sleep(SPACE_QUERY_TIMEOUT_MS / 1000)

    respuestas = _space_responses.pop(query_id, [])

    loop = asyncio.new_event_loop()
    for archivo in huerfanos:
        loop.run_until_complete(rereplicate_archivo(archivo, respuestas))
    loop.close()


# ── USER_REGISTERED ───────────────────────────────────────────────────────────

def on_user_registered(payload: dict, sender_node_id: str) -> None:
    """Inserta el usuario en la DB local si no existe."""
    Usuario.get_or_create(
        nombre=payload["nombre"],
        defaults={
            "contrasena": payload["contrasena"],
            "rol":        payload["rol"],
            "intereses":  payload["intereses"],
        }
    )


# ── FILE_STORED ───────────────────────────────────────────────────────────────

def on_file_stored(payload: dict, sender_node_id: str) -> None:
    """
    Registra el archivo y sus ubicaciones en la DB local.
    No guarda el PDF en disco — solo los metadatos.
    """
    try:
        propietario = Usuario.get(Usuario.nombre == payload["propietario"])
    except Usuario.DoesNotExist:
        return  # No conocemos al usuario todavía, ignorar

    archivo, _ = Archivo.get_or_create(
        hash_archivo=payload["hash_archivo"],
        defaults={
            "nombre":       payload["nombre"],
            "categoria":    payload["categoria"],
            "subcategoria": payload["subcategoria"],
            "confianza":    payload["confianza"],
            "propietario":  propietario,
        }
    )

    # Registrar ubicación primaria
    UbicacionArchivo.get_or_create(
        nodo=payload["nodo_primario"],
        archivo=archivo,
        defaults={"es_replica": False}
    )

    # Registrar ubicación réplica
    UbicacionArchivo.get_or_create(
        nodo=payload["nodo_replica"],
        archivo=archivo,
        defaults={"es_replica": True}
    )


# ── FILE_DELETED ──────────────────────────────────────────────────────────────

def on_file_deleted(payload: dict, sender_node_id: str) -> None:
    """
    Elimina el archivo del disco local y de la DB si este nodo lo tiene.
    """
    from backend.services.storage import eliminar_pdf
    try:
        archivo = Archivo.get(Archivo.hash_archivo == payload["hash_archivo"])
    except Archivo.DoesNotExist:
        return  # Ya no lo teníamos, nada que hacer

    eliminar_pdf(archivo.nombre, payload["propietario"])
    archivo.delete_instance(recursive=True)


# ── SPACE_QUERY ───────────────────────────────────────────────────────────────

def on_space_query(payload: dict, sender_node_id: str) -> None:
    """
    Responde con nuestro espacio disponible directamente
    al nodo que preguntó, vía DEALER/ROUTER (punto a punto).
    """
    import zmq
    from backend.config import NODE_IP

    query_id = payload["query_id"]
    espacio  = obtener_espacio_disponible()

    # Buscar la IP del nodo que preguntó
    try:
        nodo_origen = Nodo.get(Nodo.id == sender_node_id)
    except Nodo.DoesNotExist:
        return

    respuesta = make_space_response(query_id, espacio, sender_node_id)

    context = zmq.Context.instance()
    sock = context.socket(zmq.DEALER)
    sock.connect(f"tcp://{nodo_origen.ip}:{ZMQ_DIRECT_PORT}")
    # DEALER envía: [empty, message]
    sock.send_multipart([b"", respuesta.encode()])
    sock.close()


# ── SPACE_RESPONSE ────────────────────────────────────────────────────────────

def on_space_response(payload: dict, sender_node_id: str) -> None:
    """
    Acumula respuestas de espacio para el query_id correspondiente.
    replication.py las leerá desde _space_responses.
    """
    # Solo procesar respuestas dirigidas a este nodo
    if payload.get("target_node_id") != NODE_ID:
        return

    query_id = payload["query_id"]
    if query_id not in _space_responses:
        _space_responses[query_id] = []

    _space_responses[query_id].append({
        "node_id": sender_node_id,
        "espacio": payload["espacio"],
    })


# ── API para que replication.py lea las respuestas ───────────────────────────

def get_space_responses(query_id: str) -> list[dict]:
    return _space_responses.get(query_id, [])


def clear_space_responses(query_id: str) -> None:
    _space_responses.pop(query_id, None)

# ── SYNC_REQUEST ──────────────────────────────────────────────────────────────

def on_sync_request(payload: dict, sender_node_id: str) -> None:
    """
    Solo responde si el mensaje va dirigido a este nodo.
    Serializa el estado completo y lo manda punto a punto.
    """
    if payload.get("target_node_id") != NODE_ID:
        return

    # Serializar usuarios
    usuarios = [
        {
            "nombre":     u.nombre,
            "contrasena": u.contrasena,
            "rol":        u.rol,
            "intereses":  u.intereses,
        }
        for u in Usuario.select()
    ]

    # Serializar archivos con sus ubicaciones
    archivos = []
    for a in Archivo.select():
        ubicaciones = [
            {"nodo_id": u.nodo_id, "es_replica": u.es_replica}
            for u in a.ubicaciones
        ]
        archivos.append({
            "nombre":       a.nombre,
            "categoria":    a.categoria,
            "subcategoria": a.subcategoria,
            "confianza":    a.confianza,
            "hash_archivo": a.hash_archivo,
            "fecha_subida": a.fecha_subida.isoformat(),
            "propietario":  a.propietario.nombre,
            "ubicaciones":  ubicaciones,
        })

    # Serializar nodos
    nodos = [
        {
            "id":                n.id,
            "ip":               n.ip,
            "espacio_disponible": n.espacio_disponible,
            "activo":           n.activo,
        }
        for n in Nodo.select()
    ]

    respuesta = make_sync_response(sender_node_id, {
        "usuarios": usuarios,
        "archivos": archivos,
        "nodos":    nodos,
    })

    # Enviar punto a punto al nodo que preguntó
    try:
        nodo_origen = Nodo.get(Nodo.id == sender_node_id)
    except Nodo.DoesNotExist:
        return

    import zmq
    context = zmq.Context.instance()
    sock = context.socket(zmq.DEALER)
    sock.connect(f"tcp://{nodo_origen.ip}:{ZMQ_DIRECT_PORT}")
    sock.send_multipart([b"", respuesta.encode()])
    sock.close()


# ── SYNC_RESPONSE ─────────────────────────────────────────────────────────────

def on_sync_response(payload: dict, sender_node_id: str) -> None:
    """
    Recibe el estado completo y reconcilia DB local y disco.
    Solo procesa si va dirigido a este nodo.
    """
    if payload.get("target_node_id") != NODE_ID:
        return

    from backend.services.storage import eliminar_pdf, leer_pdf
    from datetime import datetime, timezone

    # 1. Sincronizar usuarios
    for u in payload["usuarios"]:
        Usuario.get_or_create(
            nombre=u["nombre"],
            defaults={
                "contrasena": u["contrasena"],
                "rol":        u["rol"],
                "intereses":  u["intereses"],
            }
        )

    # 2. Sincronizar nodos
    for n in payload["nodos"]:
        Nodo.insert(
            id=n["id"],
            ip=n["ip"],
            espacio_disponible=n["espacio_disponible"],
            activo=n["activo"],
            ultima_vez_visto=datetime.now(timezone.utc),
        ).on_conflict(
            conflict_target=[Nodo.id],
            update={
                Nodo.ip:                n["ip"],
                Nodo.espacio_disponible: n["espacio_disponible"],
                Nodo.activo:            n["activo"],
            }
        ).execute()

    # 3. Sincronizar archivos y detectar inconsistencias
    hashes_red = {a["hash_archivo"] for a in payload["archivos"]}

    # 3a. Archivos que tengo en disco pero fueron eliminados en la red
    #     → eliminar de disco y de DB local
    for archivo_local in Archivo.select():
        if archivo_local.hash_archivo not in hashes_red:
            propietario = archivo_local.propietario.nombre
            eliminar_pdf(archivo_local.nombre, propietario)
            archivo_local.delete_instance(recursive=True)

    # 3b. Insertar/actualizar archivos de la red
    for a in payload["archivos"]:
        try:
            propietario = Usuario.get(Usuario.nombre == a["propietario"])
        except Usuario.DoesNotExist:
            continue

        archivo, _ = Archivo.get_or_create(
            hash_archivo=a["hash_archivo"],
            defaults={
                "nombre":       a["nombre"],
                "categoria":    a["categoria"],
                "subcategoria": a["subcategoria"],
                "confianza":    a["confianza"],
                "propietario":  propietario,
                "fecha_subida": datetime.fromisoformat(a["fecha_subida"]),
            }
        )

        for ub in a["ubicaciones"]:
            UbicacionArchivo.get_or_create(
                nodo=ub["nodo_id"],
                archivo=archivo,
                defaults={"es_replica": ub["es_replica"]}
            )

        # 3c. El archivo debe estar físicamente en este nodo pero no está
        #     → pedirlo al nodo que sí lo tiene
        este_nodo_debe_tenerlo = any(
            ub["nodo_id"] == NODE_ID for ub in a["ubicaciones"]
        )
        if este_nodo_debe_tenerlo:
            pdf = leer_pdf(a["nombre"], a["propietario"])
            if pdf is None:
                _solicitar_archivo(archivo, a["ubicaciones"]) # TODO: tal vez no es necesario


def _solicitar_archivo(archivo, ubicaciones: list[dict]) -> None:
    """
    Pide el PDF físico a uno de los nodos que lo tienen.
    Se lanza en un hilo para no bloquear la sincronización.
    """
    import threading
    threading.Thread(
        target=_fetch_archivo_sync,
        args=(archivo, ubicaciones),
        daemon=True
    ).start()


def _fetch_archivo_sync(archivo, ubicaciones: list[dict]) -> None:
    import asyncio
    from backend.services.replication import enviar_pdf_a_nodo
    from backend.config import NODE_IP

    nodos_activos = {n.id: n for n in Nodo.select().where(Nodo.activo == True)}

    for ub in ubicaciones:
        nodo_id = ub["nodo_id"]
        if nodo_id == NODE_ID:
            continue
        nodo = nodos_activos.get(nodo_id)
        if nodo is None:
            continue

        # Pedir el PDF vía HTTP al nodo fuente
        import httpx, asyncio
        async def fetch():
            url = f"http://{nodo.ip}:8000/files/internal/download"
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.get(url, params={
                    "nombre":   archivo.nombre,
                    "usuario":  archivo.propietario.nombre,
                })
                if r.status_code == 200:
                    from backend.services.storage import guardar_pdf
                    guardar_pdf(r.content, archivo.nombre, archivo.propietario.nombre)
                    return True
            return False

        loop = asyncio.new_event_loop()
        ok = loop.run_until_complete(fetch())
        loop.close()
        if ok:
            break