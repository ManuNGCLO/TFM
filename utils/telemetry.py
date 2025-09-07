# app/utils/telemetry.py
from __future__ import annotations
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

# ---- Configuración ----------------------------------------------------------
# Habilita/deshabilita telemetría sin tocar código.
ENABLED = os.getenv("ENABLE_TELEMETRY", "1") not in {"0", "false", "False"}

# Compatibilidad con tu ruta anterior: data/history_nl2cypher.jsonl
LOG_DIR_DEFAULT = "data"
LOG_FILE_DEFAULT = "history_nl2cypher.jsonl"

LOG_DIR = os.getenv("TELEMETRY_DIR", LOG_DIR_DEFAULT)
LOG_FILE = os.getenv("TELEMETRY_FILE", LOG_FILE_DEFAULT)
LOG_PATH = os.path.join(LOG_DIR, LOG_FILE)


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        # Nunca romper por telemetría
        pass


def _safe_append_jsonl(path: str, record: Dict[str, Any]) -> None:
    """
    Escribe una línea JSON en `path`. Cualquier error se ignora silenciosamente
    para no afectar a la app.
    """
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        # No interrumpir el flujo por problemas de escritura
        pass


# -----------------------------------------------------------------------------
# API principal (firma idéntica a la que ya usas)
# -----------------------------------------------------------------------------
def log_event(
    event_type: str,              # "generate" | "execute"
    question: str,
    engine: str,                  # "Reglas locales" | "GPT (OpenAI)" | "Auto (GPT+Rescate)"
    model: Optional[str],         # p.ej. "gpt-4o-mini" o None
    cypher: Optional[str],
    status: str,                  # "ok" | "fallback" | "error"
    rows: Optional[int] = None,
    ms: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    """
    Registro compatible con tu implementación previa, con mejoras:
    - UTC (sufijo Z)
    - tolerante a fallos
    - configurable por entorno
    """
    if not ENABLED:
        return

    try:
        _ensure_dir(LOG_DIR)
        rec = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "type": event_type,
            "question": question,
            "engine": engine,
            "model": model,
            "cypher": cypher,
            "status": status,
            "rows": rows,
            "ms": ms,
            "error": error,
        }
        _safe_append_jsonl(LOG_PATH, rec)
    except Exception:
        # No romper jamás por telemetría
        pass


# -----------------------------------------------------------------------------
# Helper opcional, por si algún día quieres loguear eventos genéricos
# sin toda la estructura NL→Cypher.
# -----------------------------------------------------------------------------
def log_simple(event: str, page: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
    """
    Registro minimalista (opcional). No lo usa tu flujo actual, pero puede ser útil.
    """
    if not ENABLED:
        return

    try:
        _ensure_dir(LOG_DIR)
        rec = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "event": event,
            "page": page,
            "metadata": metadata or {},
        }
        _safe_append_jsonl(LOG_PATH, rec)
    except Exception:
        pass
