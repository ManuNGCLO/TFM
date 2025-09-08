# pages/3_🧠_Respuesta_Explicada.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import re
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st

from utils.graph_client import run_cypher
from utils.text_to_cypher import gen as nl2cypher

st.set_page_config(page_title="Respuesta explicada con citas", layout="wide")
st.title("🧠 Respuesta explicada con citas del documento")
st.caption(
    "Pregunta en lenguaje natural → recuperamos documentos y artículos ↴ "
    "extraemos fragmentos relevantes y, redactamos una respuesta breve con citas."
)

# ─────────────────────────────────────────────────────────────────────────────
# Utilidades de texto (normalización, ranking, snippets)
# ─────────────────────────────────────────────────────────────────────────────
def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
    s = re.sub(r"[^a-z0-9\s/.-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokenize(q: str) -> List[str]:
    s = _normalize(q)
    toks = [t for t in re.split(r"\W+", s) if t and len(t) > 2]
    return toks

def _score(text: str, query_tokens: List[str]) -> float:
    if not text:
        return 0.0
    t = _normalize(text)
    score = 0
    for tok in query_tokens:
        score += t.count(tok)
    # penaliza textos muy largos
    return score / math.sqrt(max(1, len(t)))

def _best_snippets(text: str, query_tokens: List[str], max_snips: int = 3, window: int = 240) -> List[str]:
    if not text:
        return []
    low = _normalize(text)
    snips: List[str] = []
    used = set()
    for tok in query_tokens:
        i = low.find(tok)
        if i == -1 or tok in used:
            continue
        used.add(tok)
        start = max(0, i - window // 2)
        end = min(len(text), i + window // 2)
        chunk = text[start:end].strip()
        snips.append(chunk)
        if len(snips) >= max_snips:
            break
    # dedupe ligero
    out, seen = [], set()
    for s in snips:
        k = _normalize(s)[:60]
        if k not in seen:
            out.append(s)
            seen.add(k)
    return out

# ─────────────────────────────────────────────────────────────────────────────
# OpenAI opcional
# ─────────────────────────────────────────────────────────────────────────────
def _openai_client():
    api_key = (st.secrets.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None
    try:
        from openai import OpenAI
        return ("v1", OpenAI(api_key=api_key))
    except Exception:
        try:
            import openai
            openai.api_key = api_key
            return ("v0", openai)
        except Exception:
            return None

def _llm_answer(question: str, context_blocks: List[Tuple[str, str, str]], model_hint: str = "gpt-4o-mini") -> Optional[str]:
    client_tuple = _openai_client()
    if not client_tuple:
        return None

    version, client = client_tuple
    parts = []
    for doc_id, art_num, snip in context_blocks:
        tag = f"[{doc_id} • Art. {art_num}]"
        parts.append(f"{tag}\n{snip}")
    context_text = "\n\n---\n\n".join(parts)

    system = (
        "Eres un asistente jurídico. Responde CONCISO (máx. 5 frases) usando únicamente el contexto. "
        "Incluye referencias entre corchetes con el formato [DOC • Art. N]. "
        "Si no hay evidencia suficiente en el contexto, indícalo."
    )
    user = f"Pregunta: {question}\n\nContexto:\n{context_text}"

    try:
        if version == "v1":
            msg = client.chat.completions.create(
                model=model_hint,
                temperature=0.1,
                messages=[{"role":"system","content":system},{"role":"user","content":user}],
            )
            return (msg.choices[0].message.content or "").strip()
        else:
            content = client.ChatCompletion.create(
                model=model_hint, temperature=0.1,
                messages=[{"role":"system","content":system},{"role":"user","content":user}]
            )["choices"][0]["message"]["content"]
            return (content or "").strip()
    except Exception as e:
        st.warning(f"No pude llamar al modelo ({model_hint}): {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# Recuperación desde el grafo
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=90)
def _retrieve_docs_for_question(q: str, limit_docs: int = 8) -> pd.DataFrame:
    """
    Usa tus reglas NL→Cypher para obtener documentos candidatos (id, titulo).
    Si no devuelve esas columnas, hace fallback a fulltext (sanitizado para Lucene).
    """
    cy = nl2cypher(q)
    df = run_cypher(cy).to_data_frame()

    cols = [c.lower() for c in df.columns]
    df.columns = cols
    if {"id","titulo"}.issubset(set(cols)):
        out = df[["id","titulo"]].drop_duplicates().head(limit_docs)
    elif "documento" in cols and "titulo" in cols:
        out = df.rename(columns={"documento":"id"})[["id","titulo"]].drop_duplicates().head(limit_docs)
    elif "doc" in cols:
        out = df.rename(columns={"doc":"id"})
        out["titulo"] = out.get("titulo", out["id"])
        out = out[["id","titulo"]].drop_duplicates().head(limit_docs)
    else:
        # fallback: fulltext (¡sanitizado para evitar errores Lucene!)
        qn = _normalize(q)
        safe_q = re.sub(r"[^a-z0-9\s]", " ", qn)  # elimina '/', '?', etc.
        q_ft = """
        CALL db.index.fulltext.queryNodes('doc_fulltext', $q) YIELD node AS d, score
        RETURN d.id AS id, d.titulo AS titulo
        ORDER BY score DESC
        LIMIT $k
        """
        out = run_cypher(q_ft, parameters={"q": safe_q, "k": limit_docs}).to_data_frame()
    return out

@st.cache_data(show_spinner=False, ttl=90)
def _load_articles_for_docs(doc_ids: List[str]) -> pd.DataFrame:
    """Carga artículos con su texto para un conjunto de documentos."""
    if not doc_ids:
        return pd.DataFrame(columns=["doc_id","doc_titulo","art_num","art_id","texto"])
    q = """
    MATCH (d:Documento)-[:TIENE_ARTICULO]->(a:Articulo)
    WHERE d.id IN $ids
    RETURN d.id AS doc_id, d.titulo AS doc_titulo,
           a.numero AS art_num, a.id AS art_id, a.texto AS texto
    ORDER BY d.titulo, toInteger(coalesce(a.numero,'0')), a.titulo
    """
    return run_cypher(q, parameters={"ids": doc_ids}).to_data_frame()

# ─────────────────────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────────────────────
q = st.text_area(
    "Pregunta en lenguaje natural",
    placeholder="Ej.: ¿Quién deroga la LO 15/1999 y en qué artículo se indica?",
    height=110,
)

c1, c2, c3 = st.columns([1,1,1])
with c1:
    k_docs = st.number_input("Máx. documentos", 1, 20, 6)
with c2:
    k_snips = st.number_input("Máx. fragmentos (contexto)", 1, 10, 4)
with c3:
    model = st.selectbox("Modelo (si hay API key)", ["gpt-4o-mini","gpt-4o","gpt-4.1-mini","o3-mini"])

if st.button("Responder", type="primary"):
    if not q.strip():
        st.warning("Escribe una pregunta.")
        st.stop()

    with st.spinner("Buscando documentos relevantes…"):
        df_docs = _retrieve_docs_for_question(q, limit_docs=int(k_docs))

    if df_docs.empty:
        st.error("No encontré documentos relacionados.")
        st.stop()

    st.success(f"Documentos candidatos: {len(df_docs)}")
    st.dataframe(df_docs, hide_index=True, use_container_width=True)

    with st.spinner("Analizando artículos…"):
        df_art = _load_articles_for_docs(df_docs["id"].tolist())

    if df_art.empty:
        st.error("Los documentos no tienen artículos con texto.")
        st.stop()

    toks = _tokenize(q)
    df_art["score"] = df_art["texto"].apply(lambda t: _score(t, toks))
    df_art = df_art.sort_values("score", ascending=False)
    df_top = df_art.head(int(k_snips)).copy()
    df_top["snippet"] = df_top["texto"].apply(lambda t: "… " + " … ".join(_best_snippets(t, toks)) + " …")

    st.markdown("#### Fragmentos relevantes")
    show_cols = ["doc_id","doc_titulo","art_num","snippet"]
    st.dataframe(df_top[show_cols], hide_index=True, use_container_width=True)

    # Construye contexto (doc_id, art_num, snippet)
    ctx_blocks = list(zip(df_top["doc_id"], df_top["art_num"], df_top["snippet"]))

    st.markdown("### Respuesta")
    answer = _llm_answer(q, ctx_blocks, model_hint=model)
    if answer:
        st.info(answer)
        if st.button("Copiar respuesta"):
            st.code(answer, language=None)
    else:
        # Respuesta extractiva (sin LLM)
        bullets = []
        for doc_id, art, snip in ctx_blocks:
            bullets.append(f"• Posible respuesta en **{doc_id} • Art. {art}**:\n> {snip}")
        st.write("\n\n".join(bullets))
        st.caption("Sin API key: se muestran fragmentos como evidencia.")

    # Exportar citas
    csv = df_top[["doc_id","doc_titulo","art_num","snippet"]].to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Descargar citas (CSV)", data=csv, file_name="citas_respuesta.csv", mime="text/csv")
