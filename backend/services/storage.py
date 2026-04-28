import shutil
from pathlib import Path
from config import STORAGE_DIR, NODE_ID, STORAGE_LIMIT_BYTES


# ── Rutas ─────────────────────────────────────────────────────────────────────

def _ruta_archivo(nombre_archivo: str, usuario: str) -> Path:
    """
    Cada usuario tiene su propia subcarpeta dentro de storage/.
    Ej: storage/juan/documento.pdf
    """
    carpeta = STORAGE_DIR / usuario
    carpeta.mkdir(parents=True, exist_ok=True)
    return carpeta / nombre_archivo


# ── Guardar / eliminar ────────────────────────────────────────────────────────

def guardar_pdf(pdf_bytes: bytes, nombre_archivo: str, usuario: str) -> Path:
    """Guarda el PDF en disco. Retorna la ruta absoluta."""
    ruta = _ruta_archivo(nombre_archivo, usuario)
    ruta.write_bytes(pdf_bytes)
    return ruta


def eliminar_pdf(nombre_archivo: str, usuario: str) -> bool:
    """Elimina el PDF del disco. Retorna True si existía."""
    ruta = _ruta_archivo(nombre_archivo, usuario)
    if ruta.exists():
        ruta.unlink()
        return True
    return False


def leer_pdf(nombre_archivo: str, usuario: str) -> bytes | None:
    """Lee el PDF del disco. Retorna None si no existe."""
    ruta = _ruta_archivo(nombre_archivo, usuario)
    if ruta.exists():
        return ruta.read_bytes()
    return None


def archivo_existe(nombre_archivo: str, usuario: str) -> bool:
    return _ruta_archivo(nombre_archivo, usuario).exists()


# ── Espacio disponible ────────────────────────────────────────────────────────

def obtener_espacio_usado() -> int:
    """Bytes usados dentro de STORAGE_DIR."""
    return sum(f.stat().st_size for f in STORAGE_DIR.rglob("*") if f.is_file())


def obtener_espacio_disponible() -> int:
    """Bytes libres dentro del límite definido en config."""
    usado = obtener_espacio_usado()
    return max(0, STORAGE_LIMIT_BYTES - usado)


def hay_espacio_para(pdf_bytes: bytes) -> bool:
    """Verifica si el PDF cabe dentro del espacio disponible."""
    return len(pdf_bytes) <= obtener_espacio_disponible()


# ── Listar archivos locales de un usuario ────────────────────────────────────

def listar_pdfs_usuario(usuario: str) -> list[str]:
    """Retorna los nombres de archivo que están físicamente en este nodo."""
    carpeta = STORAGE_DIR / usuario
    if not carpeta.exists():
        return []
    return [f.name for f in carpeta.iterdir() if f.suffix == ".pdf"]