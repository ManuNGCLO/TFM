# app/ingest/pdf_to_json.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List, Optional
import re
import os
import unicodedata

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

# ------------------ utilidades ------------------ #

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return s

def _filename_no_ext(name: Optional[str]) -> str:
    if not name:
        return "documento"
    base = os.path.basename(name)
    return os.path.splitext(base)[0]

def _iso_date_from_text(text: str) -> Optional[str]:
    # YYYY-MM-DD
    m = re.search(r"\b(20\d{2}|19\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b", text)
    if m:
        return m.group(0)
    # dd/mm/yyyy
    m = re.search(r"\b(0?[1-9]|[12]\d|3[01])/(0?[1-9]|1[0-2])/(20\d{2}|19\d{2})\b", text)
    if m:
        d, mth, y = m.groups()
        return f"{int(y):04d}-{int(mth):02d}-{int(d):02d}"
    # “14 de diciembre de 1999”
    m = re.search(r"(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚáéíóú]+)\s+de\s+(20\d{2}|19\d{2})", text)
    if m:
        d = int(m.group(1)); mon = _norm(m.group(2)); y = int(m.group(3))
        meses = {
            "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
            "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,
            "noviembre":11,"diciembre":12
        }
        mth = meses.get(mon, 1)
        return f"{y:04d}-{mth:02d}-{d:02d}"
    return None

def _guess_tipo(text: str) -> str:
    t = _norm(text)
    if "ley organica" in t or "ley orgánica" in text.lower():
        return "Ley Orgánica"
    if "reglamento" in t:
        return "Reglamento"
    if "memoria" in t:
        return "Memoria"
    if "boletin oficial" in t or "boletín oficial" in text.lower():
        return "Boletín"
    return "Documento"

_RGPD_PATTERNS = [r"\brgpd\b", r"\bgdpr\b", r"reglamento\s*\(?ue\)?\s*2016/679", r"2016/679"]
_LO3_PATTERNS  = [r"\bley\s*org[aá]nica\s*3/2018\b", r"\blo\s*3/2018\b"]
_LO15_PATTERNS = [r"\bley\s*org[aá]nica\s*15/1999\b", r"\blo\s*15/1999\b"]

def _find(text: str, pats: List[str]) -> bool:
    t = _norm(text)
    return any(re.search(p, t) for p in pats)

# ------------------ extracción de artículos ------------------ #

_ART_RE = re.compile(
    r"(^|\n)\s*(art[ií]culo|art\.)\s+(\d+[a-z]?)\s*[\.:-]?\s*(.+?)\s*(?=\n|$)",
    flags=re.IGNORECASE
)

def _extract_articles_general(text: str) -> List[Dict[str, Any]]:
    """Busca encabezados 'Artículo N Título...' y crea artículos."""
    arts: List[Dict[str, Any]] = []
    for m in _ART_RE.finditer(text):
        num = m.group(3).strip()
        tit = m.group(4).strip()
        if len(tit) > 180:
            tit = tit[:177] + "…"
        arts.append({"numero": num, "titulo": tit})
    # quita duplicados por pies/cabeceras repetidos
    seen = set()
    out = []
    for a in arts:
        k = (a["numero"], a["titulo"])
        if k not in seen:
            out.append(a); seen.add(k)
    return out

def _extract_articles_blocks(text: str) -> List[Dict[str, Any]]:
    """Para DOUE/CELEX: usa el mismo patrón y, si no hay, intenta 'CAPÍTULO/TÍTULO'."""
    arts = _extract_articles_general(text)
    if arts:
        return arts
    chap_re = re.compile(r"(cap[ií]tulo|t[ií]tulo)\s+[ivx0-9]+", re.IGNORECASE)
    hits = [m.group(0).strip() for m in chap_re.finditer(text)]
    return [{"numero": str(i+1), "titulo": h} for i, h in enumerate(hits[:10])]

# ------------------ relaciones/documentos mencionados ------------------ #

def _extract_relaciones(text: str) -> List[Dict[str, str]]:
    rels: List[Dict[str, str]] = []
    t = _norm(text)

    for m in re.finditer(r"\bderog(a|an|o)\b\s+(la\s+)?([^\n\.;,]{5,120})", t):
        frag = m.group(3).strip()
        frag = re.split(r"\b(en|de|por|mediante)\b", frag)[0].strip()
        if frag:
            rels.append({"tipo": "DEROGA", "documento": frag})

    for m in re.finditer(r"\bmodific(a|an|o)\b\s+(la\s+)?([^\n\.;,]{5,120})", t):
        frag = m.group(3).strip()
        frag = re.split(r"\b(en|de|por|mediante)\b", frag)[0].strip()
        if frag:
            rels.append({"tipo": "MODIFICA", "documento": frag})

    # dedup
    seen = set(); out = []
    for r in rels:
        k = (r["tipo"], r["documento"])
        if k not in seen:
            out.append(r); seen.add(k)
    return out

# ------------------ API principal ------------------ #

def pdf_to_doc(
    file_bytes: bytes,
    try_relations: bool = True,
    block_mode: bool = False,
    hint: Optional[str] = None,
    file_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Devuelve un JSON con cabecera del documento, artículos, temas, relaciones, menciones."""
    if fitz is None:
        raise RuntimeError("PyMuPDF (fitz) no está instalado. Instala 'pymupdf'.")

    doc = fitz.open(stream=file_bytes, filetype="pdf")
    text_parts: List[str] = []
    for p in doc:
        text_parts.append(p.get_text("text"))
    doc.close()

    full_text = "\n".join(text_parts)
    first_line = next((ln.strip() for ln in full_text.splitlines() if ln.strip()), "")

    base_name = _filename_no_ext(file_name or "upload.pdf")
    titulo = hint or (first_line if 8 <= len(first_line) <= 200 else base_name)
    tipo = _guess_tipo(full_text)
    fecha = _iso_date_from_text(full_text)

    temas: List[str] = []
    menciona_doc: List[str] = []
    if "proteccion de datos" in _norm(full_text) or "protección de datos" in full_text.lower():
        temas.append("Protección de datos")
    if _find(full_text, _RGPD_PATTERNS):
        menciona_doc.append("Reglamento (UE) 2016/679")
    if _find(full_text, _LO3_PATTERNS):
        menciona_doc.append("LO 3/2018")
    if _find(full_text, _LO15_PATTERNS):
        menciona_doc.append("LO 15/1999")

    articulos = _extract_articles_blocks(full_text) if block_mode else _extract_articles_general(full_text)
    relaciones = _extract_relaciones(full_text) if try_relations else []

    return {
        "titulo": titulo or base_name or "Documento",
        "tipo": tipo or "Documento",
        "fecha": fecha,
        "vigente": True,
        "temas": sorted(set(temas)),
        "entidades": [],
        "relaciones": relaciones,
        "menciona_doc": sorted(set(menciona_doc)),
        "articulos": articulos,
    }
