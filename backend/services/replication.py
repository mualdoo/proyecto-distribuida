"""
Lógica de replicación.

Este módulo orquesta el proceso completo al subir un PDF:
  1. Pide a todos los nodos su espacio disponible  (vía messaging)
  2. Elige los 2 nodos con más espacio
  3. Envía el PDF al nodo primario y al nodo réplica
  4. Actualiza UbicacionArchivo en la DB local
  5. Anuncia FILE_STORED a todos los nodos

También contiene la lógica de re-replicación cuando un nodo
se desconecta y sus archivos quedan sin réplica suficiente.

NOTA: Las funciones que dependen de messaging se completan
en la Fase 4, aquí dejamos los stubs con su firma definida.
"""

from backend.db.models import Archivo, UbicacionArchivo, Nodo
from backend.services.storage import guardar_pdf, eliminar_pdf, leer_pdf, obtener_espacio_disponible
from backend.config import NODE_ID


# ── Selección de nodos destino ────────────────────────────────────────────────

def elegir_nodos_destino(respuestas_espacio: list[dict]) -> tuple[dict, dict]:
    """
    Recibe una lista de respuestas de los nodos con su espacio disponible:
        [{"node_id": str, "espacio": float}, ...]

    Retorna los dos nodos con más espacio como (primario, replica).
    Lanza ValueError si hay menos de 2 nodos disponibles.
    """
    if len(respuestas_espacio) < 2:
        raise ValueError("Se necesitan al menos 2 nodos activos para replicar.")

    ordenados = sorted(respuestas_espacio, key=lambda n: n["espacio"], reverse=True)
    return ordenados[0], ordenados[1]


# ── Guardar en nodo remoto ────────────────────────────────────────────────────

async def enviar_pdf_a_nodo(nodo_ip: str, pdf_bytes: bytes,
                             nombre_archivo: str, usuario: str) -> bool:
    """
    Envía el PDF a un nodo remoto vía HTTP (POST /internal/upload).
    Retorna True si el nodo confirmó la recepción.

    STUB — se implementa completamente en Fase 5 cuando
    el endpoint /internal/upload esté definido en FastAPI.
    """
    import httpx
    url = f"http://{nodo_ip}:8000/internal/upload"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(url, files={
                "file": (nombre_archivo, pdf_bytes, "application/pdf")
            }, data={"usuario": usuario})
        return response.status_code == 200
    except Exception:
        return False


# ── Registro en DB ────────────────────────────────────────────────────────────

def registrar_ubicacion(archivo: Archivo, nodo_id: str, es_replica: bool) -> None:
    """Inserta o ignora una UbicacionArchivo."""
    UbicacionArchivo.get_or_create(
        nodo=nodo_id,
        archivo=archivo,
        defaults={"es_replica": es_replica}
    )


# ── Re-replicación tras desconexión ──────────────────────────────────────────

def obtener_archivos_sin_replica(nodo_id_desconectado: str) -> list[Archivo]:
    """
    Retorna los Archivos que solo tenían ubicación en el nodo desconectado
    y ya no tienen ninguna réplica activa en otro nodo.
    """
    nodos_activos = {n.id for n in Nodo.select().where(Nodo.activo == True)}

    archivos_huerfanos = []
    ubicaciones = (UbicacionArchivo
                   .select()
                   .where(UbicacionArchivo.nodo == nodo_id_desconectado))

    for ubicacion in ubicaciones:
        archivo = ubicacion.archivo
        # Buscar si hay otra copia en un nodo activo
        otras = (UbicacionArchivo
                 .select()
                 .where(
                     (UbicacionArchivo.archivo == archivo) &
                     (UbicacionArchivo.nodo != nodo_id_desconectado)
                 ))
        tiene_copia_activa = any(u.nodo_id in nodos_activos for u in otras)
        if not tiene_copia_activa:
            archivos_huerfanos.append(archivo)

    return archivos_huerfanos


async def rereplicate_archivo(archivo: Archivo,
                               respuestas_espacio: list[dict]) -> bool:
    """
    Intenta replicar un archivo huérfano en un nodo activo disponible.
    Retorna True si logró replicar.
    """
    # Buscar la copia que sí existe en algún nodo activo
    nodos_activos = {n.id: n for n in Nodo.select().where(Nodo.activo == True)}
    fuente = None
    for ubicacion in archivo.ubicaciones:
        if ubicacion.nodo_id in nodos_activos:
            fuente = nodos_activos[ubicacion.nodo_id]
            break

    if fuente is None:
        return False  # No hay ninguna copia accesible

    # Leer el PDF desde el nodo fuente
    pdf_bytes = leer_pdf(archivo.nombre, archivo.propietario.nombre)
    if pdf_bytes is None:
        return False

    # Elegir un nodo destino con espacio (excluyendo los que ya tienen el archivo)
    nodos_con_copia = {u.nodo_id for u in archivo.ubicaciones}
    candidatos = [r for r in respuestas_espacio if r["node_id"] not in nodos_con_copia]

    if not candidatos:
        return False

    destino = max(candidatos, key=lambda n: n["espacio"])
    nodo_destino = nodos_activos.get(destino["node_id"])
    if nodo_destino is None:
        return False

    exito = await enviar_pdf_a_nodo(
        nodo_destino.ip, pdf_bytes,
        archivo.nombre, archivo.propietario.nombre
    )
    if exito:
        registrar_ubicacion(archivo, nodo_destino.id, es_replica=True)

    return exito