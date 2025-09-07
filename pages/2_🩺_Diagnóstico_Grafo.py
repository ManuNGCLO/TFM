# -*- coding: utf-8 -*-
# app/pages/2_🩺_Diagnóstico_Grafo.py
from __future__ import annotations

import pandas as pd
import streamlit as st

# Import flexible según tu proyecto
try:
    from utils.graph_client import get_graph, run_cypher
except Exception:
    from utils.graph_client import get_graph, run_cypher  # fallback

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Diagnóstico del Grafo", layout="wide")
st.title("🩺 Diagnóstico del Grafo")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _df_safe(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    # Evita problemas Arrow con listas/dicts/bytes
    def _fix(x):
        import collections
        if isinstance(x, (list, tuple, set, dict, bytes, bytearray)):
            try:
                if isinstance(x, (bytes, bytearray)):
                    return x.decode("utf-8", "ignore")
                if isinstance(x, dict):
                    return str(x)
                return ", ".join(map(str, x)) if not isinstance(x, dict) else str(x)
            except Exception:
                return str(x)
        return x
    for c in df.columns:
        try:
            if df[c].map(lambda v: isinstance(v, (list, tuple, set, dict, bytes, bytearray))).any():
                df[c] = df[c].map(_fix)
        except Exception:
            pass
    return df

def _csv_button(df: pd.DataFrame, name: str):
    if df.empty:
        return
    st.download_button(
        "⬇️ Descargar CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=name,
        mime="text/csv",
        use_container_width=True,
    )

@st.cache_data(ttl=60, show_spinner=False)
def _scalar(q: str, **params):
    try:
        df = run_cypher(q, **params).to_data_frame()
        if df is None or df.empty:
            return 0
        # toma la primera columna del primer registro
        return list(df.iloc[0].values)[0]
    except Exception:
        return 0

@st.cache_data(ttl=60, show_spinner=False)
def _table(q: str, **params) -> pd.DataFrame:
    try:
        return _df_safe(run_cypher(q, **params).to_data_frame())
    except Exception:
        return pd.DataFrame()

# ──────────────────────────────────────────────────────────────────────────────
# Conexión y versión
# ──────────────────────────────────────────────────────────────────────────────
with st.spinner("Conectando a Neo4j…"):
    try:
        g = get_graph()
        st.success("Conexión a Neo4j: OK ✅")
    except Exception as e:
        st.error(f"No pude conectar a Neo4j: {e}")
        st.stop()

try:
    vdf = _table("CALL dbms.components() YIELD name, versions, edition "
                 "RETURN name, versions[0] AS version, edition")
    if not vdf.empty:
        st.caption(f"Servidor: **{vdf.iloc[0]['name']} {vdf.iloc[0]['version']}**, Edición: {vdf.iloc[0]['edition']}")
except Exception:
    st.caption("No se pudo leer la versión (puede requerir permisos).")

st.divider()

# ──────────────────────────────────────────────────────────────────────────────
# Métricas generales
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Métricas generales")
c1, c2, c3, c4 = st.columns(4)
nodes = _scalar("MATCH (n) RETURN count(n) AS n")
rels = _scalar("MATCH ()-[r]->() RETURN count(r) AS n")
isol = _scalar("MATCH (n) WHERE NOT (n)--() RETURN count(n) AS n")
c1.metric("Nodos", nodes)
c2.metric("Relaciones", rels)
c3.metric("Componentes (WCC)", "—")  # Si usas GDS, aquí puedes calcularlo
c4.metric("Nodos aislados", isol)

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# Métricas del dominio
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Métricas del dominio")
docs_dom = _scalar("""
MATCH (d:Documento)-[:TIENE_ARTICULO]->(:Articulo)
RETURN count(DISTINCT d) AS n
""")
arts_total = _scalar("""
MATCH (:Documento)-[:TIENE_ARTICULO]->(a:Articulo)
RETURN count(a) AS n
""")
vigentes_dom = _scalar("""
MATCH (d:Documento {vigente:true})-[:TIENE_ARTICULO]->(:Articulo)
RETURN count(DISTINCT d) AS n
""")
avg_art = (arts_total / docs_dom) if docs_dom else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Documentos (con artículos)", docs_dom)
c2.metric("Artículos", arts_total)
c3.metric("Artículos/doc (avg)", f"{avg_art:.2f}")
c4.metric("Vigentes (con artículos)", vigentes_dom)

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# Relaciones por tipo
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Relaciones por tipo")
rels_df = _table("""
MATCH ()-[r]->()
RETURN type(r) AS tipo, count(*) AS total
ORDER BY total DESC
""")
st.dataframe(rels_df, use_container_width=True, height=300)
_csv_button(rels_df, "relaciones_por_tipo.csv")

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# Top documentos por nº de artículos
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Top documentos por nº de artículos")
top_docs = _table("""
MATCH (d:Documento)-[:TIENE_ARTICULO]->(a:Articulo)
RETURN d.id AS id, d.titulo AS titulo, count(a) AS num_articulos
ORDER BY num_articulos DESC, titulo
LIMIT 50
""")
st.dataframe(top_docs, use_container_width=True, height=340)
_csv_button(top_docs, "top_docs_por_articulos.csv")

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# Top documentos mencionados (MENCIONA_DOC)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Top documentos mencionados (MENCIONA_DOC)")
mdf = _table("""
MATCH (:Documento)-[:MENCIONA_DOC]->(d2:Documento)
OPTIONAL MATCH (d2)-[:TIENE_ARTICULO]->(a:Articulo)
RETURN d2.id AS id, d2.titulo AS titulo, count(*) AS menciones, count(a) AS num_articulos
ORDER BY menciones DESC, titulo
LIMIT 20
""")
st.dataframe(mdf, use_container_width=True, height=340)
_csv_button(mdf, "top_docs_mencionados.csv")

st.divider()

# ──────────────────────────────────────────────────────────────────────────────
# Integridad del dominio
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Integridad del dominio")

with st.expander("Artículos huérfanos (sin TIENE_ARTICULO desde Documento)"):
    orf = _table("""
MATCH (a:Articulo)
WHERE NOT ()-[:TIENE_ARTICULO]->(a)
RETURN a.id AS id, a.numero AS numero, left(coalesce(a.texto,''), 140) AS preview
ORDER BY id
LIMIT 500
""")
    st.write(f"Encontrados: **{len(orf)}**")
    st.dataframe(orf, use_container_width=True, height=260)
    _csv_button(orf, "articulos_huerfanos.csv")

with st.expander("Violaciones de tipos en relaciones del dominio"):
    st.caption("Cada relación debería conectar tipos específicos. Aquí listamos los casos que **no** cumplen:")
    checks = {
        "DEROGA":       "NOT (x:Documento AND y:Documento)",
        "MODIFICA":     "NOT (x:Documento AND y:Documento)",
        "TRATA_SOBRE":  "NOT (x:Documento AND y:Tema)",
        "MENCIONA":     "NOT (x:Documento AND y:Entidad)",
        "MENCIONA_DOC": "NOT (x:Documento AND y:Documento)",
        "TIENE_ARTICULO":"NOT (x:Documento AND y:Articulo)",
    }
    for rel, cond in checks.items():
        bad = _table(f"""
MATCH (x)-[r:`{rel}`]->(y)
WHERE {cond}
RETURN labels(x) AS src_labels,
       type(r)   AS rel,
       labels(y) AS dst_labels,
       coalesce(x.id, x.nombre, x.titulo) AS src_key,
       coalesce(y.id, y.nombre, y.titulo) AS dst_key
LIMIT 300
""")
        st.markdown(f"**{rel}** → violaciones: **{len(bad)}**")
        if not bad.empty:
            st.dataframe(bad, use_container_width=True, height=220)
            _csv_button(bad, f"violaciones_{rel.lower()}.csv")
        st.markdown("")

with st.expander("Documentos sin título"):
    nt = _table("""
MATCH (d:Documento)
WHERE coalesce(trim(d.titulo),'') = ''
RETURN d.id AS id
ORDER BY id
LIMIT 300
""")
    st.write(f"Encontrados: **{len(nt)}**")
    st.dataframe(nt, use_container_width=True, height=220)
    _csv_button(nt, "documentos_sin_titulo.csv")

st.divider()

# ──────────────────────────────────────────────────────────────────────────────
# Mantenimiento (opcional)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("⚙️ Mantenimiento")

cA, cB = st.columns(2)

with cA:
    if st.button("Crear índices/constraints recomendados"):
        try:
            run_cypher("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Documento) REQUIRE d.id IS UNIQUE")
            run_cypher("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Articulo)  REQUIRE a.id IS UNIQUE")
            run_cypher("CREATE INDEX IF NOT EXISTS FOR (e:Entidad)       ON (e.nombre)")
            run_cypher("CREATE INDEX IF NOT EXISTS FOR (t:Tema)          ON (t.nombre)")
            st.success("Índices/constraints creados (o ya existían).")
        except Exception as e:
            st.error(f"Error creando índices/constraints: {e}")

with cB:
    st.caption("Acciones de limpieza (seguras):")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🧹 Eliminar :Articulo huérfano"):
            try:
                run_cypher("""
MATCH (a:Articulo)
WHERE NOT ()-[:TIENE_ARTICULO]->(a)
DETACH DELETE a
""")
                st.success("Eliminados artículos huérfanos.")
            except Exception as e:
                st.error(f"Error: {e}")
    with col2:
        if st.button("🧹 Eliminar :Documento sin artículos"):
            try:
                run_cypher("""
MATCH (d:Documento)
WHERE NOT (d)-[:TIENE_ARTICULO]->()
DETACH DELETE d
""")
                st.success("Eliminados documentos sin artículos.")
            except Exception as e:
                st.error(f"Error: {e}")

st.caption("Consejo: si usas Neo4j GDS, puedes añadir métricas de componentes/conectividad para enriquecer este panel.")
