"""
Endpoints para subir, listar y eliminar archivos del usuario.
También expone endpoints internos para transferencias entre nodos.
"""

import uuid
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from fastapi.responses import Response

from backend.db.models import Archivo, UbicacionArchivo, Nodo
from backend.services.classifier import procesar_pdf
from backend.services.storage import (
    guardar_pdf, eliminar_pdf, leer_pdf,
    hay_espacio_para, obtener_espacio_disponible,
)
from backend.services.replication import (
    elegir_nodos_destino, enviar_pdf_a_nodo, registrar_ubicacion
)
from backend.messaging.broadcaster import publish
from backend.messaging.protocol import (
    make_file_stored, make_space_query
)
from backend.messaging.handlers import get_space_responses, clear_space_responses
from backend.api.auth import get_usuario_actual, require_admin
from backend.config import NODE_ID, SPACE_QUERY_TIMEOUT_MS

import asyncio
router = APIRouter(prefix="/files", tags=["files"])


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _consultar_espacio() -> list[dict]:
    """
    Publica SPACE_QUERY y espera SPACE_QUERY_TIMEOUT_MS
    para acumular respuestas de los demás nodos.
    """
    query_id = str(uuid.uuid4())
    publish(make_space_query(query_id))
    await asyncio.sleep(SPACE_QUERY_TIMEOUT_MS / 1000)
    respuestas = get_space_responses(query_id)
    clear_space_responses(query_id)
    return respuestas


# ── Subir PDF ─────────────────────────────────────────────────────────────────

@router.post("/upload", status_code=201)
async def upload(
    file: UploadFile = File(...),
    usuario=Depends(get_usuario_actual),
):
    pdf_bytes = await file.read()

    # Validar espacio local
    if not hay_espacio_para(pdf_bytes):
        raise HTTPException(status_code=507, detail="Espacio insuficiente en este nodo.")

    intereses = usuario.intereses.split(",") if usuario.intereses else []

    # Clasificar
    resultado = procesar_pdf(pdf_bytes, intereses)

    # Verificar duplicado por hash + propietario
    if (Archivo.select()
               .where(
                   (Archivo.hash_archivo == resultado["hash"]) &
                   (Archivo.propietario == usuario.id)
               ).exists()):
        raise HTTPException(status_code=409, detail="Ya subiste este archivo.")

    # Consultar espacio en la red para elegir nodos destino
    respuestas = await _consultar_espacio()

    # Incluir este nodo en las opciones
    respuestas.append({"node_id": NODE_ID, "espacio": obtener_espacio_disponible()})

    try:
        nodo_primario, nodo_replica = elegir_nodos_destino(respuestas)
    except ValueError as e:
        # raise HTTPException(status_code=503, detail=str(e))
        # TODO: Pruebas con un nodo
        nodo_primario = {"node_id": NODE_ID, "espacio": obtener_espacio_disponible()}
        nodo_replica  = {"node_id": NODE_ID, "espacio": obtener_espacio_disponible()}

    # Guardar en nodo primario
    nombre_archivo = file.filename
    if nodo_primario["node_id"] == NODE_ID:
        guardar_pdf(pdf_bytes, nombre_archivo, usuario.nombre)
    else:
        nodo = Nodo.get(Nodo.id == nodo_primario["node_id"])
        ok = await enviar_pdf_a_nodo(nodo.ip, pdf_bytes, nombre_archivo, usuario.nombre)
        if not ok:
            raise HTTPException(status_code=503, detail="No se pudo guardar en el nodo primario.")

    # Guardar réplica
    if nodo_replica["node_id"] == NODE_ID:
        guardar_pdf(pdf_bytes, nombre_archivo, usuario.nombre)
    else:
        nodo = Nodo.get(Nodo.id == nodo_replica["node_id"])
        await enviar_pdf_a_nodo(nodo.ip, pdf_bytes, nombre_archivo, usuario.nombre)

    # Registrar en DB local
    archivo = Archivo.create(
        nombre=nombre_archivo,
        categoria=resultado["categoria"],
        subcategoria=resultado["subcategoria"],
        confianza=resultado["confianza"],
        hash_archivo=resultado["hash"],
        propietario=usuario,
    )
    registrar_ubicacion(archivo, nodo_primario["node_id"], es_replica=False) # ERROR
    registrar_ubicacion(archivo, nodo_replica["node_id"],  es_replica=True)

    # Anunciar a la red
    publish(make_file_stored(
        archivo_id=archivo.id,
        nombre=nombre_archivo,
        categoria=resultado["categoria"],
        subcategoria=resultado["subcategoria"],
        confianza=resultado["confianza"],
        hash_archivo=resultado["hash"],
        propietario=usuario.nombre,
        nodo_primario=nodo_primario["node_id"],
        nodo_replica=nodo_replica["node_id"],
    ))

    return {
        "mensaje":      "Archivo subido correctamente.",
        "categoria":    resultado["categoria"],
        "subcategoria": resultado["subcategoria"],
        "confianza":    resultado["confianza"],
        "nodo_primario": nodo_primario["node_id"],
        "nodo_replica":  nodo_replica["node_id"],
    }


# ── Listar archivos del usuario ───────────────────────────────────────────────

@router.get("/")
def listar(
    categoria: str | None = None,
    usuario=Depends(get_usuario_actual),
):
    query = Archivo.select().where(Archivo.propietario == usuario.id)
    if categoria:
        query = query.where(Archivo.categoria == categoria)

    return [
        {
            "id":           a.id,
            "nombre":       a.nombre,
            "categoria":    a.categoria,
            "subcategoria": a.subcategoria,
            "confianza":    a.confianza,
            "fecha_subida": a.fecha_subida.isoformat(),
            "ubicaciones": [
                {"nodo_id": u.nodo_id, "es_replica": u.es_replica}
                for u in a.ubicaciones
            ],
        }
        for a in query
    ]


# ── Descargar PDF ─────────────────────────────────────────────────────────────

@router.get("/{archivo_id}/download")
def download(archivo_id: int, usuario=Depends(get_usuario_actual)):
    try:
        archivo = Archivo.get(
            (Archivo.id == archivo_id) &
            (Archivo.propietario == usuario.id)
        )
    except Archivo.DoesNotExist:
        raise HTTPException(status_code=404, detail="Archivo no encontrado.")

    # Buscar una copia física en algún nodo activo
    nodos_activos = {n.id: n for n in Nodo.select().where(Nodo.activo == True)}

    for ubicacion in archivo.ubicaciones:
        if ubicacion.nodo_id == NODE_ID:
            pdf = leer_pdf(archivo.nombre, usuario.nombre)
            if pdf:
                return Response(
                    content=pdf,
                    media_type="application/pdf",
                    headers={"Content-Disposition": f'attachment; filename="{archivo.nombre}"'},
                )
        elif ubicacion.nodo_id in nodos_activos:
            # Redirigir transparentemente al nodo que tiene el archivo
            nodo = nodos_activos[ubicacion.nodo_id]
            from fastapi.responses import RedirectResponse
            return RedirectResponse(
                url=f"http://{nodo.ip}:8000/files/{archivo_id}/download"
            )

    raise HTTPException(status_code=503, detail="Archivo no disponible en ningún nodo activo.")


# ── Eliminar PDF ──────────────────────────────────────────────────────────────

@router.delete("/{archivo_id}")
def eliminar(archivo_id: int, usuario=Depends(get_usuario_actual)):
    try:
        archivo = Archivo.get(
            (Archivo.id == archivo_id) &
            (Archivo.propietario == usuario.id)
        )
    except Archivo.DoesNotExist:
        raise HTTPException(status_code=404, detail="Archivo no encontrado.")

    _eliminar_archivo_completo(archivo, usuario.nombre)
    return {"mensaje": "Archivo eliminado."}


def _eliminar_archivo_completo(archivo: Archivo, nombre_usuario: str) -> None:
    """Elimina el PDF de todos los nodos donde esté y borra el registro de DB."""
    from backend.messaging.protocol import build
    nodos_activos = {n.id: n for n in Nodo.select().where(Nodo.activo == True)}

    for ubicacion in archivo.ubicaciones:
        if ubicacion.nodo_id == NODE_ID:
            eliminar_pdf(archivo.nombre, nombre_usuario)
        elif ubicacion.nodo_id in nodos_activos:
            nodo = nodos_activos[ubicacion.nodo_id]
            import httpx, asyncio
            async def _delete_remote():
                async with httpx.AsyncClient(timeout=10) as client:
                    await client.delete(
                        f"http://{nodo.ip}:8000/internal/delete",
                        params={"nombre": archivo.nombre, "usuario": nombre_usuario}
                    )
            asyncio.get_event_loop().run_until_complete(_delete_remote())

    # Anunciar eliminación a toda la red
    from backend.messaging.protocol import MSG_FILE_STORED
    publish(build("FILE_DELETED", {
        "hash_archivo": archivo.hash_archivo,
        "propietario":  nombre_usuario,
    }))

    archivo.delete_instance(recursive=True)


# ── Endpoints internos (nodo a nodo) ─────────────────────────────────────────

@router.post("/internal/upload")
async def internal_upload(
    file: UploadFile = File(...),
    usuario: str = "",
):
    """Recibe un PDF de otro nodo para almacenarlo localmente."""
    pdf_bytes = await file.read()
    guardar_pdf(pdf_bytes, file.filename, usuario)
    return {"ok": True}


@router.get("/internal/download")
def internal_download(nombre: str, usuario: str):
    """Sirve un PDF a otro nodo que lo solicita."""
    pdf = leer_pdf(nombre, usuario)
    if pdf is None:
        raise HTTPException(status_code=404, detail="Archivo no encontrado localmente.")
    return Response(content=pdf, media_type="application/pdf")


@router.delete("/internal/delete")
def internal_delete(nombre: str, usuario: str):
    """Elimina un PDF del disco local por instrucción de otro nodo."""
    eliminar_pdf(nombre, usuario)
    return {"ok": True}