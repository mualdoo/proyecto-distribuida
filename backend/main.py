"""
Punto de entrada — arranca FastAPI y el subsistema ZeroMQ.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.db.database import init_db
from backend.db.models import MODELS
from backend.messaging.node import start as start_node
from backend.api.auth  import router as auth_router
from backend.api.files import router as files_router
from backend.api.nodes import router as nodes_router
from backend.api.admin import router as admin_router
from backend.config import NODE_ID, NODE_IP, API_HOST, API_PORT


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Arranque ──────────────────────────────────────────────────────────────
    init_db(MODELS)
    start_node()
    print(f"[main] Nodo {NODE_ID} listo — API en http://{NODE_IP}:{API_PORT}")
    yield
    # ── Cierre (el goodbye lo maneja node.py vía atexit) ─────────────────────


app = FastAPI(title="Scientific Docs Node", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(files_router)
app.include_router(nodes_router)
app.include_router(admin_router)