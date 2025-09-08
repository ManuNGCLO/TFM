# utils/graph_client.py
# -*- coding: utf-8 -*-
"""
Cliente Neo4j para TFM R46 — SIN pasos manuales en Aura
Mejoras clave:
- Auto-esquema idempotente: constraints + índices + full-text (doc/tema/artículo)
- Detección de TLS por esquema (bolt+s / bolt+ssc / neo4j+s / neo4j+ssc)
- Conexión cacheada (Streamlit) con smoke-test y reconexión automática
- API práctica: run(), run_cypher(), run_data(), evaluate(), get_graph()
- Tolerante a Neo4j 4.x/5.x para full-text (DDL / procedimiento)

Lee credenciales de st.secrets o del entorno:
  NEO4J_URI, NEO4J_USER, NEO4J_PASS | NEO4J_PASSWORD
"""
from __future__ import annotations
import os
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from py2neo import Graph
from py2neo.errors import ConnectionBroken
try:
    from py2neo.wiring import BrokenWireError  # py2neo 2021+
except Exception:  # compat fallback
    class BrokenWireError(Exception): ...

# -------------------------------------------------------------------
# Credenciales y TLS
# -------------------------------------------------------------------

def _read_creds() -> Tuple[str, str, str]:
    """Lee credenciales desde st.secrets o variables de entorno."""
    secrets = getattr(st, "secrets", {})
    uri = secrets.get("NEO4J_URI") or os.getenv("NEO4J_URI", "")
    user = secrets.get("NEO4J_USER") or os.getenv("NEO4J_USER", "")
    password = (
        secrets.get("NEO4J_PASS")
        or secrets.get("NEO4J_PASSWORD")
        or os.getenv("NEO4J_PASS", "")
        or os.getenv("NEO4J_PASSWORD", "")
    )
    return uri, user, password


def _parse_security(uri: str) -> Tuple[bool, bool]:
    """Devuelve (secure, verify) según el esquema (+s = TLS, +ssc = sin verificación)."""
    u = (uri or "").lower()
    secure = "+s" in u
    verify = True
    if "+ssc" in u:
        verify = False
    return secure, verify

# -------------------------------------------------------------------
# Esquema automático (constraints, índices y full-text)
# -------------------------------------------------------------------

def _safe_run(g: Graph, query: str) -> None:
    try:
        g.run(query).consume()
    except Exception:
        # ignora "ya existe" o sintaxis no soportada en ciertas versiones
        pass


def _ensure_schema(g: Graph) -> None:
    """Crea constraints/índices/full-text si no existen (idempotente)."""
    base = [
        "CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (d:Documento) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT art_id IF NOT EXISTS FOR (a:Articulo)  REQUIRE a.id IS UNIQUE",
        "CREATE CONSTRAINT tema_nombre IF NOT EXISTS FOR (t:Tema) REQUIRE t.nombre IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (d:Documento) ON (d.titulo)",
        "CREATE INDEX IF NOT EXISTS FOR (t:Tema)       ON (t.norm)",
        "CREATE INDEX IF NOT EXISTS FOR (e:Entidad)    ON (e.norm)",
        "CREATE INDEX IF NOT EXISTS FOR (a:Articulo)   ON (a.numero)",
    ]
    for q in base:
        _safe_run(g, q)

    # Full-text (DDL Neo4j 5.x)
    _safe_run(g, "CREATE FULLTEXT INDEX doc_fulltext  IF NOT EXISTS FOR (d:Documento) ON EACH [d.titulo, d.id, d.alias]")
    _safe_run(g, "CREATE FULLTEXT INDEX tema_fulltext IF NOT EXISTS FOR (t:Tema)       ON EACH [t.nombre, t.alias]")
    _safe_run(g, "CREATE FULLTEXT INDEX ft_articulos  IF NOT EXISTS FOR (a:Articulo)   ON EACH [a.titulo, a.texto]")

    # Si tu instancia no soporta DDL FT (Neo4j 4.x), cae al procedimiento
    try:
        g.run("CALL db.index.fulltext.list()").data()
    except Exception:
        _safe_run(g, "CALL db.index.fulltext.createNodeIndex('doc_fulltext',['Documento'],['titulo','id','alias'])")
        _safe_run(g, "CALL db.index.fulltext.createNodeIndex('tema_fulltext',['Tema'],['nombre','alias'])")
        _safe_run(g, "CALL db.index.fulltext.createNodeIndex('ft_articulos',['Articulo'],['titulo','texto'])")

# -------------------------------------------------------------------
# Conexión cacheada + smoke test
# -------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def _connect(uri: str, user: str, password: str) -> Graph:
    if not (uri and user and password):
        st.error("Faltan credenciales de Neo4j. Define NEO4J_URI / NEO4J_USER / NEO4J_PASS en .streamlit/secrets.toml")
        st.stop()
    secure, verify = _parse_security(uri)
    g = Graph(uri, auth=(user, password), secure=secure, verify=verify)
    g.run("RETURN 1").evaluate()       # smoke test
    _ensure_schema(g)                  # crea esquema al primer uso
    return g

_cached_graph: Optional[Graph] = None

def _new_graph() -> Graph:
    uri, user, password = _read_creds()
    return _connect(uri, user, password)

# -------------------------------------------------------------------
# API pública
# -------------------------------------------------------------------

def get_graph(force_new: bool = False) -> Graph:
    """Devuelve un Graph operativo; si está roto, reconecta."""
    global _cached_graph
    if force_new or _cached_graph is None:
        _cached_graph = _new_graph()
        return _cached_graph
    try:
        _cached_graph.run("RETURN 1").evaluate()
        return _cached_graph
    except (ConnectionBroken, BrokenWireError):
        _cached_graph = _new_graph()
        return _cached_graph


def run(query: str, parameters: Optional[Dict[str, Any]] = None):
    """Ejecuta Cypher con reconexión automática. Retorna Cursor de py2neo."""
    g = get_graph()
    try:
        return g.run(query, parameters=parameters or {})
    except (ConnectionBroken, BrokenWireError):
        g = get_graph(force_new=True)
        return g.run(query, parameters=parameters or {})

# ✅ Alias retro-compatible para tu página: mantiene import run_cypher
def run_cypher(query: str, parameters: Optional[Dict[str, Any]] = None):
    return run(query, parameters)

def run_data(query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Ejecuta Cypher y devuelve data() (lista de dicts)."""
    return run(query, parameters).data()

def evaluate(query: str, parameters: Optional[Dict[str, Any]] = None):
    """Ejecuta Cypher y devuelve evaluate()."""
    return run(query, parameters).evaluate()
