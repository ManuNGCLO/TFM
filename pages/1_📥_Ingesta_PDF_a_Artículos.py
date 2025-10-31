# app/pages/1_📥_Ingesta_PDF_a_Artículos.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import io
import re
from dataclasses import dataclass
from typing import List, Tuple
import pandas as pd
import streamlit as st

from utils.graph_client import run_cypher
from utils.text_to_cypher import gen  # ✅ ahora acepta doc_id opcional

# ═════════════════════════════════════════════════════════════════════════════
# Configuración de página
# ═════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Ingesta PDF → Artículos", layout="wide")
st.title("📥 Ingesta de PDF a Artículos")

st.caption(
    "Sube uno o varios PDF de normas. El sistema crea/actualiza el nodo `(:Documento)` y "
    "sus `(:Articulo)` enlazados con `[:TIENE_ARTICULO]`. Además puede generar automáticamente "
    "las relaciones semánticas (`DEROGA`, `MODIFICA`, `MENCIONA`, `TRATA_SOBRE`, etc.)."
)

# ═════════════════════════════════════════════════════════════════════════════
# Utilidades
# ═════════════════════════════════════════════════════════════════════════════
def slugify(s: str) -> str:
    s = s or ""
    s = s.strip().lower()
    s = re.sub(r"[^\w]+", "-", s, flags=re.UNICODE)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-") or "documento"


@st.cache_data(show_spinner=False, ttl=300)
def extract_text_from_pdf(file_bytes: bytes) -> str:
    """Extrae texto del PDF usando PyPDF2 o PyMuPDF como fallback."""
    # Intento 1: PyPDF2
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(file_bytes))
        pages = [p.extract_text() or "" for p in reader.pages]
        text = "\n".join(pages)
        if text.strip():
            return text
    except Exception:
        pass

    # Intento 2: PyMuPDF
    try:
        import fitz
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        return "\n".join([page.get_text() for page in doc])
    except Exception:
        return ""


@dataclass
class Articulo:
    numero: str
    texto: str


def split_articles(text: str) -> List[Articulo]:
    """Detecta encabezados 'Artículo n.' o 'Art. n.' y separa el texto."""
    if not text:
        return []
    text = text.replace("\r", "")
    lines = text.split("\n")

    pat = re.compile(r"(?im)^\s*(art[íi]culo|art\.)\s+(\d+[a-z]?)\.?\s*(.*)$")
    idxs = [i for i, ln in enumerate(lines) if pat.match(ln.strip())]
    if not idxs:
        return []

    arts: List[Articulo] = []
    for i, start in enumerate(idxs):
        end = idxs[i + 1] if i + 1 < len(idxs) else len(lines)
        m = pat.match(lines[start].strip())
        numero = m.group(2) if m else f"{i+1}"
        body = "\n".join(lines[start:end]).strip()
        body_lines = body.split("\n", 1)
        if body_lines and pat.match(body_lines[0].strip()) and len(body_lines) > 1:
            body = body_lines[1].strip()
        arts.append(Articulo(numero=numero, texto=body))
    return arts


def ensure_document(doc_id_hint: str, doc_title_hint: str) -> str:
    """Crea o asegura un nodo :Documento y devuelve su id definitivo."""
    doc_id = slugify(doc_id_hint or doc_title_hint)
    q = """
MERGE (d:Documento {id:$id})
ON CREATE SET d.titulo = coalesce($titulo, $id)
ON MATCH  SET d.titulo = coalesce($titulo, d.titulo)
RETURN d.id AS id
"""
    df = run_cypher(q, parameters={"id": doc_id, "titulo": (doc_title_hint or doc_id)}).to_data_frame()
    return df.iloc[0]["id"] if not df.empty else doc_id


def upsert_articles_for_document(doc_id: str, arts: List[Articulo]) -> Tuple[int, int]:
    """Inserta o actualiza artículos para un documento (Neo4j 5 compatible)."""
    cy = """
MATCH (d:Documento {id:$doc})
WITH d, COUNT { (d)-[:TIENE_ARTICULO]->(:Articulo) } AS before
UNWIND $arts AS art
MERGE (a:Articulo {id: d.id + '-art-' + art.numero})
ON CREATE SET a.numero = art.numero, a.texto = art.texto
ON MATCH  SET a.numero = art.numero, a.texto = art.texto
MERGE (d)-[:TIENE_ARTICULO]->(a)
WITH d, before
MATCH (d)-[:TIENE_ARTICULO]->(x:Articulo)
RETURN before, COUNT(x) AS after
"""
    payload = [{"numero": a.numero, "texto": a.texto} for a in arts]
    df = run_cypher(cy, parameters={"doc": doc_id, "arts": payload}).to_data_frame()
    if df.empty:
        return 0, 0
    before = int(df.iloc[0]["before"])
    after = int(df.iloc[0]["after"])
    nuevos = max(after - before, 0)
    return nuevos, after


# ═════════════════════════════════════════════════════════════════════════════
# UI principal de ingesta
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("### Subir PDF(s)")
files = st.file_uploader("Selecciona uno o varios PDF", type=["pdf"], accept_multiple_files=True)

if files:
    st.info("Puedes ajustar el **ID del documento** y su **título** antes de ingresar.")
    for i, f in enumerate(files):
        with st.container(border=True):
            c1, c2 = st.columns([2, 3])
            with c1:
                st.text_input("Nombre del archivo", f.name, disabled=True, key=f"fn_{i}")
            with c2:
                stem = f.name.rsplit(".", 1)[0]
                doc_id_hint = st.text_input("ID del Documento (editable)", value=slugify(stem), key=f"id_{i}")
                doc_title = st.text_input("Título del Documento (editable)", value=stem, key=f"title_{i}")

            if st.button("Procesar e ingresar", key=f"ing_{i}", type="primary"):
                try:
                    raw = f.read()
                    text = extract_text_from_pdf(raw)
                    if not text.strip():
                        st.error("No se pudo extraer texto del PDF.")
                        continue

                    arts = split_articles(text)
                    if not arts:
                        st.warning("No se detectaron encabezados de artículos.")
                        continue

                    # 1️⃣ Crear o actualizar el Documento y sus Artículos
                    doc_id = ensure_document(doc_id_hint, doc_title)
                    nuevos, total = upsert_articles_for_document(doc_id, arts)
                    st.success(f"**{doc_id}** → Artículos nuevos: **{nuevos}** · Total ahora: **{total}**")

                    # 2️⃣ Generar relaciones semánticas automáticamente
                    with st.spinner("Analizando artículos y generando relaciones semánticas..."):
                        for art in arts:
                            cypher_block = gen(art.texto, doc_id)
                            if cypher_block:
                                run_cypher(cypher_block)
                    st.success("Relaciones semánticas generadas automáticamente ✅")

                    # 3️⃣ Mostrar vista previa de artículos
                    with st.expander("Ver artículos detectados"):
                        df_prev = pd.DataFrame([a.__dict__ for a in arts])
                        st.dataframe(df_prev, use_container_width=True, height=320)

                except Exception as e:
                    st.error(f"Error durante la ingesta: {e}")

# ═════════════════════════════════════════════════════════════════════════════
# Botón manual para reprocesar relaciones
# ═════════════════════════════════════════════════════════════════════════════
if st.button("🔍 Generar relaciones semánticas para todos los documentos"):
    try:
        with st.spinner("Analizando todos los artículos existentes..."):
            q = "MATCH (d:Documento)-[:TIENE_ARTICULO]->(a:Articulo) RETURN d.id AS doc, a.texto AS texto"
            rows = run_cypher(q).to_data_frame()
            for _, row in rows.iterrows():
                cy = gen(row["texto"], row["doc"])
                if cy:
                    run_cypher(cy)
        st.success("Relaciones semánticas generadas para todos los documentos ✅")
    except Exception as e:
        st.error(f"Error generando relaciones: {e}")

# ═════════════════════════════════════════════════════════════════════════════
# Snapshot rápido
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("📊 Conteo rápido de artículos por documento")

@st.cache_data(ttl=120, show_spinner=False)
def quick_snapshot(limit: int = 25) -> pd.DataFrame:
    q = """
MATCH (d:Documento)
OPTIONAL MATCH (d)-[:TIENE_ARTICULO]->(a:Articulo)
WITH d, count(a) AS n
RETURN d.id AS id, d.titulo AS titulo, n AS num_articulos
ORDER BY num_articulos DESC, titulo
LIMIT $lim
"""
    return run_cypher(q, parameters={"lim": limit}).to_data_frame()

colA, colB = st.columns([1, 3])
with colA:
    lim = st.slider("Top documentos", 5, 100, 25, 5)
with colB:
    if st.button("↻ Actualizar"):
        st.cache_data.clear()
df_snap = quick_snapshot(lim)
st.dataframe(df_snap, use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# Administración (mantiene todas las funciones)
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("⚙️ Administración")

with st.expander("🔴 Resetear base (BORRA TODO)"):
    st.caption("Crea constraints básicos después del borrado.")
    confirm = st.text_input("Escribe BORRAR para confirmar", key="danger")
    if st.button("Resetear base", type="primary"):
        if (confirm or "").strip().upper() == "BORRAR":
            try:
                run_cypher("MATCH (n) DETACH DELETE n")
                run_cypher("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Documento) REQUIRE d.id IS UNIQUE")
                run_cypher("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Articulo) REQUIRE a.id IS UNIQUE")
                run_cypher("CREATE INDEX IF NOT EXISTS FOR (e:Entidad) ON (e.nombre)")
                run_cypher("CREATE INDEX IF NOT EXISTS FOR (t:Tema) ON (t.nombre)")
                st.success("Base reiniciada + constraints/índices creados.")
            except Exception as e:
                st.error(f"Error al resetear: {e}")
        else:
            st.warning("Debes escribir BORRAR para confirmar.")

with st.expander("🧹 Limpieza de nodos huérfanos"):
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Eliminar :Documento sin artículos"):
            try:
                run_cypher("""
MATCH (d:Documento)
WHERE NOT (d)-[:TIENE_ARTICULO]->()
DETACH DELETE d
""")
                st.success("Eliminados documentos sin artículos.")
            except Exception as e:
                st.error(f"Error: {e}")
    with col2:
        if st.button("Eliminar :Articulo sin documento"):
            try:
                run_cypher("""
MATCH (a:Articulo)
WHERE NOT ()-[:TIENE_ARTICULO]->(a)
DETACH DELETE a
""")
                st.success("Eliminados artículos huérfanos.")
            except Exception as e:
                st.error(f"Error: {e}")
