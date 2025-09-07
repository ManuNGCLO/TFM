# app/utils/ingest_from_pdf.py
from __future__ import annotations
import re
from typing import List, Dict, Any, Tuple
import fitz  # PyMuPDF
from py2neo import Graph

SOFT_HYPHEN = "\u00ad"
NBSP = "\u00a0"
ARTICLE_RE = re.compile(
    r'(?mi)^\s*(art(?:[íi]culo|\.)\s*(\d+(?:\s*(?:bis|ter|quater|quinquies|sexies))?))'
    r'(?:\s*[-–—.:]\s*(.*)|\s*$)'
)

def _normalize_text(s: str) -> str:
    s = s.replace(SOFT_HYPHEN, "").replace(NBSP, " ")
    # normalizamos guiones y espacios "raros"
    s = re.sub(r"[\r\t]+", " ", s)
    s = re.sub(r"\s+\n", "\n", s)
    s = re.sub(r"\n\s+", "\n", s)
    s = re.sub(r"\s+", " ", s)
    # mantenemos saltos de línea para segmentar artículos
    s = s.replace(" .", ".").replace(" ,", ",")
    return s

def _pdf_to_text(pdf_bytes: bytes, mode: str = "text") -> str:
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        pages = []
        for p in doc:
            if mode == "blocks":
                blocks = p.get_text("blocks")
                blocks.sort(key=lambda b: (b[1], b[0]))
                page_text = "\n".join(b[4] for b in blocks if b[4].strip())
            else:
                page_text = p.get_text("text")
            pages.append(page_text)
    # Usamos saltos de línea para que ARTICLE_RE funcione
    return "\n".join(pages)

def _split_articles(full_text: str) -> List[Dict[str, Any]]:
    lines = full_text.splitlines()
    anchors = []
    for i, line in enumerate(lines):
        m = ARTICLE_RE.match(line)
        if m:
            numero = m.group(2).strip()
            titulo = (m.group(3) or "").strip() or "(sin título)"
            anchors.append((i, numero, titulo))
    arts: List[Dict[str, Any]] = []
    for idx, (start_i, numero, titulo) in enumerate(anchors):
        end_i = anchors[idx+1][0] if idx+1 < len(anchors) else len(lines)
        cuerpo = "\n".join(lines[start_i+1:end_i]).strip()
        arts.append({"numero": numero, "titulo": titulo, "texto": cuerpo})
    return arts

def parse_articles_from_bytes(pdf_bytes: bytes, mode: str = "text") -> List[Dict[str, Any]]:
    text = _pdf_to_text(pdf_bytes, mode=mode)
    return _split_articles(text)

def upsert_articles(graph: Graph, doc_hint: str, arts: List[Dict[str, Any]]) -> Tuple[int, int]:
    d = graph.evaluate("""
        MATCH (d:Documento)
        WHERE toLower(d.id) CONTAINS $h
           OR toLower(coalesce(d.norm, d.titulo)) CONTAINS replace($h,'-',' ')
        RETURN d LIMIT 1
    """, h=doc_hint.lower())
    if not d:
        raise ValueError(f"No se encontró :Documento que contenga «{doc_hint}» en id/titulo/norm.")
    doc_id = d["id"]

    created = updated = 0
    tx = graph.begin()
    for art in arts:
        n = (art.get("numero") or "").strip()
        t = (art.get("titulo") or "(sin título)").strip()
        x = (art.get("texto") or "").strip()
        if not n:
            # si no hay número, generamos uno incremental simple
            n = str(len(arts))
        aid = f"{doc_id}-art-{n}"

        exists = tx.evaluate("""
            MATCH (d:Documento {id:$doc_id})-[:TIENE_ARTICULO]->(a:Articulo {id:$aid})
            RETURN a
        """, doc_id=doc_id, aid=aid)

        if exists:
            tx.run("""
                MATCH (d:Documento {id:$doc_id})-[:TIENE_ARTICULO]->(a:Articulo {id:$aid})
                SET a.numero=$n, a.titulo=$t, a.texto=$x
            """, doc_id=doc_id, aid=aid, n=n, t=t, x=x)
            updated += 1
        else:
            tx.run("""
                MATCH (d:Documento {id:$doc_id})
                MERGE (d)-[:TIENE_ARTICULO]->(a:Articulo {id:$aid})
                ON CREATE SET a.numero=$n, a.titulo=$t, a.texto=$x
            """, doc_id=doc_id, aid=aid, n=n, t=t, x=x)
            created += 1
    graph.commit(tx)
    return created, updated
