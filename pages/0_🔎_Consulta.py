# -*- coding: utf-8 -*-
# pages/0_🔎_Consulta.py — versión corregida (usa run_cypher + UI pulida)
from __future__ import annotations

import io
import re
import time
from typing import Tuple, Optional

import pandas as pd
import streamlit as st

# --- Integraciones del proyecto
from utils.graph_client import get_graph, run_cypher   # <- ahora válido (alias en graph_client)
from utils.text_to_cypher import gen as rules_gen      # retrocompatible
try:
    # si existe el generador extendido (gen_ex), úsalo para reportar el motor
    from utils.text_to_cypher import gen_ex as rules_gen_ex  # (cypher, engine)
except Exception:
    rules_gen_ex = None  # noqa

try:
    from utils.telemetry import log_event  # opcional
except Exception:  # pragma: no cover
    def log_event(*_, **__):
        return None

# -----------------------------------------------------------------------------
# Page config
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Consulta IA → Cypher (Neo4j)", layout="wide")

# Forzamos la conexión y el esquema (constraints/FT) desde código, sin pasos manuales
_ = get_graph()

# **CLAVE**: aplica el ejemplo encolado ANTES de instanciar widgets
if "queued_example" in st.session_state:
    st.session_state["q_input"] = st.session_state.pop("queued_example")

# Estado inicial
st.session_state.setdefault("last_cypher", None)
st.session_state.setdefault("last_engine", "")
st.session_state.setdefault("last_question", "")
st.session_state.setdefault("q_input", "")
st.session_state.setdefault("openai_model_ui", st.secrets.get("OPENAI_MODEL", "gpt-4o-mini"))

# -----------------------------------------------------------------------------
# Estilos (compacto/pulido)
# -----------------------------------------------------------------------------
st.markdown("""
<div class="app-header">
  <span class="app-icon">🔎</span>
  <h1 class="app-title">Consulta en lenguaje natural → Cypher (Neo4j)</h1>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<style>
section.main > div.block-container{ padding-top: .6rem !important; padding-bottom: 1.0rem !important; }
.app-header{ display:flex; align-items:center; gap:.6rem; margin:.25rem 0 .6rem 0; }
.app-icon{ font-size:1.4rem; line-height:1; transform: translateY(2px); }
.app-title{ margin:0; font-weight:800; letter-spacing:-0.01em; font-size:clamp(20px, 3.2vw, 30px); line-height:1.25; }
[data-testid="stHorizontalBlock"]{ gap: 0.75rem !important; }
.stTextArea textarea{ min-height: 120px; }
.stButton>button{ width:100%; padding:.55rem .9rem; border-radius:.65rem; }
div[data-testid="stDataFrame"]{ margin: .25rem 0 1rem 0 !important; }
div.stAlert{ margin: .35rem 0 .75rem 0 !important; }
div.stCode > pre{ font-size:12.5px; line-height:1.35; }
.app-sep{ border:0; border-top:1px solid rgba(255,255,255,.09); margin: .9rem 0; }
.dataframe tbody tr th{ display:none; }
.badge{ display:inline-flex; gap:.35rem; align-items:center; font-size:.78rem; padding:.2rem .5rem; border-radius:.5rem; border:1px solid rgba(255,255,255,.15); }
.badge .dot{ width:.45rem; height:.45rem; border-radius:50%; background:currentColor; display:inline-block; }
.badge.rules{ color:#a1e3a1; }
.badge.fulltext{ color:#9bd3ff; }
.badge.openai{ color:#ffd39b; }
.copybtn{ font-size:.8rem; margin:.25rem 0 .5rem 0; }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------
def _safe_rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass

def _clear_query():
    for k in ("last_cypher","last_engine","last_question","q_input"):
        st.session_state.pop(k, None)
    _safe_rerun()

def is_safe_cypher(cy: str) -> bool:
    """Bloquea operaciones de escritura o potencialmente peligrosas."""
    bad = [" create "," merge "," delete "," set "," detach "," drop "," load csv"," call db."," call apoc.create"]
    low = f" {cy.lower()} "
    return not any(tok in low for tok in bad)

_DEF_ART_RX = re.compile(r"\b(art(?:iculo)?\s*(\d+[a-z]?))\b", re.IGNORECASE)

def _normalize_return(cy: str) -> str:
    """Si se retorna el nodo Documento directamente, normaliza a id/titulo."""
    try:
        m = re.search(r"(?is)\bMATCH\s*\(\s*([a-zA-Z][\w]*)\s*:\s*Documento\b", cy)
        if not m:
            return cy
        var = m.group(1)
        cy = re.sub(rf"(?i)\bRETURN\s+DISTINCT\s+{var}\b(?!\s*(?:\.|\s+AS))",
                    f"RETURN DISTINCT {var}.id AS id, {var}.titulo AS titulo", cy)
        cy = re.sub(rf"(?i)\bRETURN\s+{var}\b(?!\s*(?:\.|\s+AS))",
                    f"RETURN {var}.id AS id, {var}.titulo AS titulo", cy)
        cy = re.sub(rf"(?i),\s*{var}\b(?!\s*(?:\.|\s+AS))",
                    f", {var}.id AS id, {var}.titulo AS titulo", cy)
        cy = re.sub(rf"(?i)\bORDER\s+BY\s+{var}\b(?!\s*\.)", "ORDER BY titulo", cy)
        return cy
    except Exception:
        return cy

# -------- Fallbacks (por si el motor no devuelve algo aprovechable) --------
def _lo3_where(prefix: str = "WHERE") -> str:
    return f"""{prefix}
  toLower(d2.id) CONTAINS 'boe-a-2018-16673'
  OR toLower(d2.titulo) CONTAINS 'lo 3 2018'
  OR toLower(d2.titulo) CONTAINS 'ley organica 3/2018'
  OR toLower(d2.titulo) CONTAINS 'ley orgánica 3/2018'"""

def _build_theme_fallback_fulltext(term: str, keep_modifica: bool, keep_vigente: bool) -> str:
    q = []
    q.append(f"CALL db.index.fulltext.queryNodes('ft_articulos', '{term}~') YIELD node AS a")
    q.append("MATCH (d:Documento)-[:TIENE_ARTICULO]->(a)")
    if keep_modifica:
        q.append("MATCH (d)-[:MODIFICA]->(d2:Documento)")
        q.append(_lo3_where("WHERE"))
    if keep_vigente:
        q.append("WITH DISTINCT d")
        q.append("WHERE coalesce(d.vigente,true) = true")
    q.append("RETURN DISTINCT d.id AS id, d.titulo AS titulo")
    q.append("ORDER BY titulo")
    return "\n".join(q)

def _fallback_from_question(q: str) -> Optional[str]:
    qn = q.lower().strip()
    # Modifican LO 3/2018 y tratan sobre X
    if ("modific" in qn) and any(k in qn for k in ["lo 3/2018","lo 3 2018","ley organica 3/2018","ley orgánica 3/2018"]):
        m = re.search(r"sobre\s+['\"“”‘’«»]?([^'\"“”‘’«»]+)", qn)
        term = (m.group(1).strip() if m else "consentimiento")
        return _build_theme_fallback_fulltext(term, keep_modifica=True, keep_vigente=False)
    # Vigentes + derogan + rgpd
    if ("vigen" in qn) and ("derog" in qn) and any(t in qn for t in ["rgpd","2016/679","reglamento 2016/679","gdpr"]):
        return (
            """
MATCH (d:Documento)-[:DEROGA]->(:Documento)
WHERE coalesce(d.vigente,true) = true
WITH DISTINCT d
OPTIONAL MATCH (d)-[:MENCIONA_DOC]->(x:Documento)
WHERE toLower(x.id) CONTAINS 'celex-32016r0679'
   OR toLower(x.id) CONTAINS 'reglamento-ue-2016'
   OR toLower(x.titulo) CONTAINS '2016/679'
OPTIONAL MATCH (d)-[:MENCIONA]->(e:Entidad)
WHERE toLower(coalesce(e.norm,e.nombre)) CONTAINS 'rgpd'
WITH d, (x IS NOT NULL) OR (e IS NOT NULL) AS menciona_rgpd
WHERE menciona_rgpd
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo
"""
        ).strip()
    return None

# --- OpenAI (opcional)
SCHEMA_TEXT = """
Eres un generador de Cypher SOLO DE LECTURA. Esquema:

(:Documento {id, titulo, tipo, fecha, vigente})
  -[:TIENE_ARTICULO]-> (:Articulo {id, doc?, numero, titulo, texto})
(:Documento)-[:TRATA_SOBRE]->(:Tema {nombre, norm?})
(:Documento)-[:MENCIONA]->(:Entidad {nombre, norm?})
(:Documento)-[:MENCIONA_DOC]->(:Documento)
(:Documento)-[:DEROGA]->(:Documento)
(:Documento)-[:MODIFICA]->(:Documento)

Reglas:
- SOLO MATCH/OPTIONAL MATCH/WHERE/RETURN/ORDER/LIMIT. Nada de CREATE/MERGE/DELETE/SET.
- Declara SIEMPRE un alias principal `d` para (:Documento) cuyas filas quieres listar.
- Devuelve EXACTAMENTE:
  RETURN DISTINCT d.id AS id, d.titulo AS titulo
  (y si necesitas, ORDER BY titulo)
- Prohibido devolver el nodo completo `d`. Devuelve SIEMPRE las propiedades pedidas.
- Responde SOLO un bloque ```cypher ...```.
- Si no puedes, responde exactamente: FALLBACK
"""

def _openai_client():
    api_key = (st.secrets.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None
    try:
        from openai import OpenAI
        return ("v1", OpenAI(api_key=api_key))
    except Exception:
        import openai
        openai.api_key = api_key
        return ("v0", openai)

def _gpt_nl2cypher(question: str, model_hint: str | None = None) -> str:
    cli_tuple = _openai_client()
    if not cli_tuple:
        return "FALLBACK"
    version, client = cli_tuple
    model = (model_hint or st.secrets.get("OPENAI_MODEL") or "gpt-4o-mini").strip()
    try:
        if version == "v1":
            msg = client.chat.completions.create(
                model=model, temperature=0.0,
                messages=[{"role":"system","content":SCHEMA_TEXT},
                          {"role":"user","content":question.strip()}],
            )
            content = msg.choices[0].message.content or ""
        else:
            content = client.ChatCompletion.create(
                model=model, temperature=0.0,
                messages=[{"role":"system","content":SCHEMA_TEXT},
                          {"role":"user","content":question.strip()}],
            )["choices"][0]["message"]["content"]
    except Exception as e:
        st.warning(f"GPT no disponible ({model}): {e}")
        return "FALLBACK"
    m = re.search(r"```(?:cypher)?\s*([\s\S]+?)```", content, flags=re.IGNORECASE)
    if m: return m.group(1).strip()
    if "FALLBACK" in content: return "FALLBACK"
    looks_cypher = any(tok in content.lower() for tok in ["match","return"])
    return content.strip() if looks_cypher else "FALLBACK"

# -----------------------------------------------------------------------------
# Layout
# -----------------------------------------------------------------------------
c1, c2 = st.columns([3,2])
with c1:
    st.write("### Pregunta")
    q = st.text_area(
        "Describe lo que quieres consultar",
        key="q_input",
        label_visibility="collapsed",
        height=120,
        placeholder="Ej.: Documentos vigentes que derogan y mencionan RGPD",
    )
with c2:
    st.write("### Motor")
    engine = st.radio(
        "Generación de Cypher",
        options=["Auto (GPT+Rescate)", "Solo Reglas", "Solo GPT"],
        horizontal=True,
        index=0,
    )
    if engine in ("Solo GPT", "Auto (GPT+Rescate)"):
        st.selectbox("Modelo", options=["gpt-4o-mini","gpt-4o","gpt-4.1-mini","o3-mini"], key="openai_model_ui")

# Ejemplos (usan cola + rerun para no tocar q_input tras instanciar el widget)
def _use_example(text: str):
    st.session_state["queued_example"] = text
    _safe_rerun()

st.caption("Ejemplos rápidos:")
_examples = [
    "¿Qué documentos mencionan RGPD?",
    "Documentos que mencionan la Ley Orgánica 3/2018",
    "Documentos que tratan sobre Protección de Datos vigentes",
    "¿Qué deroga LO 3/2018?",
    "¿Quién deroga LO 15/1999?",
    "¿Qué artículos contiene LO 3/2018?",
    "Documentos que modifiquen LO 3/2018 y traten sobre consentimiento",
    "Documentos vigentes que derogan y mencionan RGPD",
]
cols = st.columns(len(_examples))
for i, ex in enumerate(_examples):
    cols[i].button(ex, key=f"ex_{i}", on_click=_use_example, args=(ex,))

# --------------------------------------------------------------------
# Acciones: Generar / Ejecutar / Limpiar
# --------------------------------------------------------------------
def _generate():
    question = (st.session_state.get("q_input") or "").strip()
    if not question:
        st.warning("Escribe una pregunta.")
        return
    engine_sel = engine
    try:
        cy = ""; used = ""
        # 1) Reglas (si existe gen_ex, lo usamos para reportar motor interno)
        def _rules(q: str) -> Tuple[str, str]:
            if callable(rules_gen_ex):
                cy_r, eng = rules_gen_ex(q, mode="auto")  # rules → fulltext → (gpt_stub) → help
                return cy_r, f"rules:{eng}"
            return rules_gen(q), "rules"

        if engine_sel == "Solo Reglas":
            cy, used = _rules(question)
        elif engine_sel == "Solo GPT":
            cy = _gpt_nl2cypher(question, st.session_state.get("openai_model_ui"))
            used = "openai"
        else:  # Auto (GPT+Rescate): primero GPT, luego reglas
            cy = _gpt_nl2cypher(question, st.session_state.get("openai_model_ui"))
            used = "auto(gpt)"
            if not cy or cy == "FALLBACK":
                cy, used = _rules(question)
                used = "auto(" + used + ")"

        if not cy or cy == "FALLBACK":
            fb = _fallback_from_question(question)
            if fb:
                cy = fb
                used = used + "+fallback"

        cy = _normalize_return(cy)
        st.session_state["last_cypher"] = cy
        st.session_state["last_engine"] = used
        st.session_state["last_question"] = question
        log_event("generate", question, engine_sel, st.session_state.get("openai_model_ui") if engine_sel!="Solo Reglas" else None, cy, "ok" if cy and cy!="FALLBACK" else "fallback")
    except Exception as e:
        st.session_state["last_cypher"] = None
        st.session_state["last_engine"] = ""
        log_event("generate", question, engine_sel, st.session_state.get("openai_model_ui") if engine_sel!="Solo Reglas" else None, "", "error", error=str(e))
        st.error(f"No pude generar Cypher: {e}")

def _run_and_show(cy: str) -> tuple[Optional[pd.DataFrame], str | None]:
    if not cy or not cy.strip():
        return None, "No hay Cypher para ejecutar."
    if not is_safe_cypher(cy):
        return None, "El Cypher generado contiene operaciones no permitidas (CREATE/MERGE/DELETE/SET…)."
    t0 = time.time()
    try:
        df = run_cypher(cy).to_data_frame()
        ms = int((time.time() - t0) * 1000)
        return df, f"{len(df):,} filas en {ms} ms"
    except Exception as e:
        return None, f"Error al ejecutar: {e}"

def _execute():
    q = st.session_state.get("last_question") or st.session_state.get("q_input","")
    cy = st.session_state.get("last_cypher")
    if not cy:
        st.warning("Genera primero el Cypher.")
        return

    # --- Rescate automático: si el resultado de la primera opción es pobre, probamos reglas
    if engine == "Auto (GPT+Rescate)":
        def _count_rows(cy_: str) -> int:
            try:
                return len(run_cypher(cy_).to_data_frame())
            except Exception:
                return -1
        n_first = _count_rows(cy)
        if n_first <= 1:
            cy_rules = rules_gen(q)
            cy_rules = _normalize_return(cy_rules)
            n_rules = _count_rows(cy_rules)
            if n_rules > n_first:
                cy = cy_rules
                st.info("🤝 Rescate automático: se usó Reglas locales por mejor cobertura de resultados.")
                st.session_state["last_cypher"] = cy
                st.session_state["last_engine"] = (st.session_state.get("last_engine") or "auto") + "→rules"

    df, how = _run_and_show(cy)
    if df is None:
        st.error(how)
        log_event("execute", q, engine, st.session_state.get("openai_model_ui") if engine!="Solo Reglas" else None, cy, "error", error=how)
        return

    # Badge del motor utilizado
    used = st.session_state.get("last_engine") or ""
    if used:
        if "rules" in used:
            st.markdown(f"<span class='badge rules'><span class='dot'></span>Motor: {used}</span>", unsafe_allow_html=True)
        elif "openai" in used or "gpt" in used:
            st.markdown(f"<span class='badge openai'><span class='dot'></span>Motor: {used}</span>", unsafe_allow_html=True)
        elif "fulltext" in used:
            st.markdown(f"<span class='badge fulltext'><span class='dot'></span>Motor: {used}</span>", unsafe_allow_html=True)

    # Controles de salida
    cL, cR = st.columns([4,1])
    with cL:
        st.success(how)
        st.dataframe(df, use_container_width=True, hide_index=True)
    with cR:
        # Copiar Cypher
        st.code(st.session_state.get("last_cypher") or "", language="cypher")
        st.button("📋 Copiar Cypher", use_container_width=True, on_click=lambda: st.session_state.update({"_copy": True}), key="copybtn")
        if st.session_state.get("_copy"):
            st.toast("Cypher copiado (selección rápida desde el bloque).")
            st.session_state["_copy"] = False

    # Descarga CSV
    try:
        buff = io.StringIO()
        df.to_csv(buff, index=False)
        st.download_button("⬇️ Descargar CSV", data=buff.getvalue().encode("utf-8"),
                           file_name="resultado.csv", mime="text/csv")
    except Exception:
        pass

    # Separador visual fino
    st.markdown("<hr class='app-sep'/>", unsafe_allow_html=True)

    # Telemetría
    ms_match = re.findall(r"(\d+)\s*ms", how or "")
    ms = int(ms_match[0]) if ms_match else None
    log_event("execute", q, engine, st.session_state.get("openai_model_ui") if engine!="Solo Reglas" else None, cy, "ok", rows=len(df), ms=ms)

c3, c4, c5 = st.columns([1,1,6])
with c3:
    st.button("Generar", on_click=_generate, type="primary", use_container_width=True)
with c4:
    st.button("Ejecutar", on_click=_execute, use_container_width=True)
with c5:
    st.button("Limpiar", on_click=_clear_query, use_container_width=True)

# Previsualización
cy_last = st.session_state.get("last_cypher")
if cy_last:
    st.success("Cypher generado (previsualización):")
    st.code(cy_last, language="cypher")

st.markdown("<hr class='app-sep'/>", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Diagnóstico
# -----------------------------------------------------------------------------
with st.expander("🔧 Diagnóstico de conexión"):
    try:
        g = get_graph()
        docs = g.run("MATCH (d:Documento) RETURN count(d) AS n").evaluate()
        arts = g.run("MATCH (:Documento)-[:TIENE_ARTICULO]->(a:Articulo) RETURN count(a) AS n").evaluate()
        st.write(f"Documentos: **{docs}** · Artículos: **{arts}**")
    except Exception as e:
        st.error(f"No pude conectar a Neo4j: {e}")

st.caption("Esquema: Documento/Articulo/Tema/Entidad con TIENE_ARTICULO, TRATA_SOBRE, MENCIONA, MENCIONA_DOC, DEROGA y MODIFICA.")
