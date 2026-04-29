import pickle
import hashlib
import fitz  # PyMuPDF
from pathlib import Path
from backend.config import MODEL_PATH, ENCODER_PATH


# ── Carga del modelo (una sola vez al importar) ───────────────────────────────

with open(MODEL_PATH, "rb") as f:
    _model = pickle.load(f)

with open(ENCODER_PATH, "rb") as f:
    _encoder = pickle.load(f)


# ── Extracción de texto ───────────────────────────────────────────────────────

def extraer_texto(pdf_bytes: bytes) -> str:
    """Extrae todo el texto de un PDF dado como bytes."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    texto = ""
    for pagina in doc:
        texto += pagina.get_text()
    doc.close()
    return texto.strip()


# ── Clasificación ─────────────────────────────────────────────────────────────

def clasificar(texto: str, intereses: list[str]) -> dict:
    """
    Clasifica el texto filtrando solo las categorías de interés del usuario.

    Retorna:
        {
            "categoria":    str,
            "subcategoria": str,
            "confianza":    float  # 0.0 – 1.0
        }
    """
    # El modelo devuelve probabilidades por clase
    proba = _model.predict_proba([texto])[0]
    clases = _encoder.classes_  # ej. ["redes/protocolos", "seguridad/criptografía", ...]

    # Filtrar solo las clases que corresponden a los intereses del usuario
    indices_validos = [
        i for i, clase in enumerate(clases)
        if any(clase.startswith(interes) for interes in intereses)
    ]

    if not indices_validos:
        # Si no hay match con intereses, usar la predicción global
        indices_validos = list(range(len(clases)))

    # Elegir la clase con mayor probabilidad dentro de los intereses
    idx = max(indices_validos, key=lambda i: proba[i])
    clase_elegida = clases[idx]
    confianza = float(proba[idx])

    # Separar categoria / subcategoria  (ej. "redes/protocolos")
    partes = clase_elegida.split("/", 1)
    categoria    = partes[0]
    subcategoria = partes[1] if len(partes) > 1 else ""

    return {
        "categoria":    categoria,
        "subcategoria": subcategoria,
        "confianza":    round(confianza, 4),
    }


# ── Hash del archivo ──────────────────────────────────────────────────────────

def calcular_hash(pdf_bytes: bytes) -> str:
    """SHA-256 del contenido del PDF."""
    return hashlib.sha256(pdf_bytes).hexdigest()


# ── Función principal que usa la API ─────────────────────────────────────────

def procesar_pdf(pdf_bytes: bytes, intereses: list[str]) -> dict:
    """
    Extrae texto, calcula hash y clasifica.

    Retorna:
        {
            "texto":        str,
            "hash":         str,
            "categoria":    str,
            "subcategoria": str,
            "confianza":    float
        }
    """
    texto = extraer_texto(pdf_bytes)
    hash_archivo = calcular_hash(pdf_bytes)
    clasificacion = clasificar(texto, intereses)

    return {
        "texto":        texto,
        "hash":         hash_archivo,
        **clasificacion,
    }