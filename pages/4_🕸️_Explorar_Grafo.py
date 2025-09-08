# app/pages/4_🕸️_Explorar_Grafo.py
# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import Dict, List, Optional

import os
import tempfile
import pandas as pd
import streamlit as st

from utils.graph_client import run_cypher  # usa tu cliente ya existente

# ------------------------------------------------------------
# Configuración
# ------------------------------------------------------------
st.set_page_config(page_title="Explorar Grafo", page_icon="🕸️", layout="wide")
st.title("Explorar Grafo")

st.markdown(
    """
Explora un subgrafo alrededor de un **Documento** (por `id` o `título`) o una **Entidad/Tema** (por `nombre`).  
Solo se usan relaciones del dominio: `MENCIONA_DOC`, `MENCIONA`, `TIENE_ARTICULO`, `TRATA_SOBRE`, `DEROGA`, `MODIFICA`.
    """
)

# Relaciones permitidas del dominio
RELS: List[str] = [
    "MENCIONA_DOC",
    "MENCIONA",
    "TIENE_ARTICULO",
    "TRATA_SOBRE",
    "DEROGA",
    "MODIFICA",
]

# Estilos de nodos y colores de arista
NODE_STYLE: Dict[str, Dict[str, str]] = {
    "Documento": {"color": "#22c55e", "shape": "box"},
    "Articulo":  {"color": "#60a5fa", "shape": "dot"},
    "Entidad":   {"color": "#f59e0b", "shape": "ellipse"},
    "Tema":      {"color": "#f472b6", "shape": "ellipse"},
}
EDGE_COLOR: Dict[str, str] = {
    "DEROGA": "#e11d48",
    "MODIFICA": "#f97316",
    "TRATA_SOBRE": "#a78bfa",
    "MENCIONA": "#38bdf8",
    "MENCIONA_DOC": "#34d399",
    "TIENE_ARTICULO": "#86efac",
}

# ------------------------------------------------------------
# Controles de la UI
# ------------------------------------------------------------
c1, c2 = st.columns([1.1, 3.9], gap="large")

with c1:
    modo_humano = st.selectbox(
        "Nodo de inicio",
        ("Documento (id/título)", "Entidad/Tema (nombre)"),
        index=0,
        help="Elige si empiezas desde un Documento (id o título) o una Entidad/Tema (por nombre exacto).",
    )

with c2:
    valor = st.text_input(
        "Valor",
        value="BOE-A-2018-16673",
        placeholder="Ejemplos: BOE-A-2018-16673 · 13500 · AEPD · RGPD",
    )

rels_sel = st.multiselect(
    "Relaciones a incluir",
    options=RELS,
    default=RELS,
    help="Filtra qué tipos de relaciones del dominio quieres incluir.",
)

c3, c4 = st.columns([1, 1], gap="large")
with c3:
    depth = st.slider(
        "Profundidad (saltos)",
        min_value=1, max_value=4, value=3, step=1,
        help="Número máximo de saltos desde el nodo de inicio.",
    )
with c4:
    limit = st.slider(
        "Límite de aristas",
        min_value=50, max_value=400, value=200, step=10,
        help="Para evitar grafos demasiado grandes.",
    )

c5, c6, c7 = st.columns([1, 1, 2], gap="medium")
explorar = c5.button("Explorar", type="primary")
do_reset = c6.button("Reset")

# --- Reset compatible con versiones nuevas de Streamlit ---
if do_reset:
    try:
        st.cache_data.clear()
    except Exception:
        pass
    st.session_state.clear()
    st.rerun()

# ------------------------------------------------------------
# Cypher helpers
# ------------------------------------------------------------
def cypher_edges_by_document(depth: int) -> str:
    """Subgrafo a N saltos desde un Documento identificado por id o título (case-insensitive)."""
    return f"""
CALL {{
  MATCH (d:Documento)
  WHERE toLower(toString(d.id)) = toLower(toString($val))
     OR toLower(toString(d.titulo)) = toLower(toString($val))
  MATCH p = (d)-[*1..{depth}]-(x)
  WHERE all(r IN relationships(p) WHERE type(r) IN $allowed)
  WITH relationships(p) AS rels
  UNWIND rels AS r
  WITH startNode(r) AS a, type(r) AS r_type, endNode(r) AS b
  RETURN DISTINCT labels(a)[0] AS a_label,
                  coalesce(a.id, a.nombre, a.numero) AS a_key,
                  r_type AS r_type,
                  labels(b)[0] AS b_label,
                  coalesce(b.id, b.nombre, b.numero) AS b_key
  LIMIT $limit
}}
RETURN a_label, a_key, r_type, b_label, b_key
"""

def cypher_edges_by_name(depth: int) -> str:
    """Subgrafo a N saltos desde una Entidad o Tema por nombre (case-insensitive)."""
    return f"""
CALL {{
  MATCH (start)
  WHERE (start:Entidad OR start:Tema)
    AND toLower(start.nombre) = toLower($val)
  MATCH p = (start)-[*1..{depth}]-(x)
  WHERE all(r IN relationships(p) WHERE type(r) IN $allowed)
  WITH relationships(p) AS rels
  UNWIND rels AS r
  WITH startNode(r) AS a, type(r) AS r_type, endNode(r) AS b
  RETURN DISTINCT labels(a)[0] AS a_label,
                  coalesce(a.id, a.nombre, a.numero) AS a_key,
                  r_type AS r_type,
                  labels(b)[0] AS b_label,
                  coalesce(b.id, b.nombre, b.numero) AS b_key
  LIMIT $limit
}}
RETURN a_label, a_key, r_type, b_label, b_key
"""

# ------------------------------------------------------------
# Fetch (con caché)
# ------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_edges(modo: str, val: str, depth: int, limit: int, allowed: List[str]) -> pd.DataFrame:
    """
    modo: "doc" | "ent"
    """
    q = cypher_edges_by_document(depth) if modo == "doc" else cypher_edges_by_name(depth)
    # ❗️IMPORTANTE: usar parameters={} para compatibilidad con clientes que no aceptan kwargs
    params = {"val": val, "allowed": allowed or RELS, "limit": int(limit)}
    cur = run_cypher(q, parameters=params)
    return cur.to_data_frame()

# ------------------------------------------------------------
# Visualización con PyVis (si está instalado)
# ------------------------------------------------------------
def draw_with_pyvis(df: pd.DataFrame) -> Optional[str]:
    try:
        from pyvis.network import Network
        import streamlit.components.v1 as components
    except Exception:
        st.info(
            "Para ver el grafo embebido, instala **pyvis**:\n\n"
            "`pip install pyvis`\n\n"
            "Mientras tanto, puedes revisar la tabla de aristas de más abajo."
        )
        return None

    net = Network(height="740px", bgcolor="#0e1117", font_color="white", directed=False)
    net.barnes_hut(gravity=-24000, central_gravity=0.3, spring_length=110, spring_strength=0.02)

    def node_id(label: str, key: str) -> str:
        return f"{label}|{key}"

    seen: set[str] = set()
    for _, row in df.iterrows():
        a_label = str(row["a_label"]); a_key = str(row["a_key"])
        r_type  = str(row["r_type"])
        b_label = str(row["b_label"]); b_key = str(row["b_key"])

        na = node_id(a_label, a_key)
        nb = node_id(b_label, b_key)

        # nodos
        for lbl, nid, key in ((a_label, na, a_key), (b_label, nb, b_key)):
            if nid in seen:
                continue
            seen.add(nid)
            style = NODE_STYLE.get(lbl, {"color": "#94a3b8", "shape": "dot"})
            txt = (key[:40] + "…") if len(key) > 40 else key
            net.add_node(
                nid,
                label=txt,
                title=f"{lbl}: {key}",
                color=style["color"],
                shape=style["shape"],
            )

        # arista
        edge_color = EDGE_COLOR.get(r_type, "#a1a1aa")
        net.add_edge(na, nb, label=r_type, color=edge_color, title=r_type)

    # Guardar en un temporal y embeber
    with tempfile.NamedTemporaryFile("w+", suffix=".html", delete=False) as tmp:
        tmp_path = tmp.name
    net.save_graph(tmp_path)
    with open(tmp_path, "r", encoding="utf-8") as f:
        components.html(f.read(), height=760, scrolling=True)
    return tmp_path

# ------------------------------------------------------------
# Acción principal
# ------------------------------------------------------------
if explorar:
    if not valor.strip():
        st.warning("Indica un valor para buscar (id/título de Documento o nombre de Entidad/Tema).")
        st.stop()

    modo = "doc" if modo_humano.startswith("Documento") else "ent"

    with st.spinner("Consultando Neo4j…"):
        try:
            df = fetch_edges(modo, valor.strip(), depth, limit, rels_sel or RELS)
        except Exception as e:
            st.error(f"Error al consultar Neo4j: {e}")
            st.stop()

    if df.empty:
        st.info("Sin resultados para ese punto de partida, profundidad y relaciones seleccionadas.")
    else:
        st.success(f"{len(df)} aristas encontradas.")
        html_path = draw_with_pyvis(df)

        # Ver/descargar datos
        with st.expander("Ver aristas como tabla"):
            st.dataframe(df, use_container_width=True)
            st.download_button(
                "Descargar CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="subgrafo.csv",
                mime="text/csv",
            )
            if html_path and os.path.exists(html_path):
                with open(html_path, "rb") as f:
                    st.download_button(
                        "Descargar grafo (HTML)",
                        f.read(),
                        file_name="grafo.html",
                        mime="text/html",
                    )

        # Ver/Copiar Cypher
        with st.expander("Ver consulta Cypher utilizada"):
            qtxt = cypher_edges_by_document(depth) if modo == "doc" else cypher_edges_by_name(depth)
            st.code(qtxt, language="cypher")
else:
    st.caption("Configura los parámetros y pulsa **Explorar**.")
