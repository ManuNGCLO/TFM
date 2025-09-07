# app/graph_client.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Optional, Tuple

import streamlit as st
from py2neo import Graph
from py2neo.errors import ConnectionBroken
try:
    from py2neo.wiring import BrokenWireError  # py2neo 2021+
except Exception:  # compat fallback
    class BrokenWireError(Exception): ...

# -------------------------------------------------------------------
# Credenciales y conexión
# -------------------------------------------------------------------

def _read_creds() -> Tuple[str, str, str]:
    """
    Lee credenciales desde st.secrets o variables de entorno.
    Soporta NEO4J_PASS y NEO4J_PASSWORD como alias.
    """
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
    """
    Devuelve (secure, verify) según el esquema:
      - *+s*  : TLS (cert. válido)      -> secure=True, verify=True
      - *+ssc*: TLS (self-signed cert.) -> secure=True, verify=False
    Soporta: bolt+s, bolt+ssc, neo4j+s, neo4j+ssc
    """
    u = (uri or "").lower()
    secure = "+s" in u
    verify = True
    if "+ssc" in u:
        verify = False
    return secure, verify

# -------------------------------------------------------------------
# Esquema automático (idempotente)
# -------------------------------------------------------------------

def _ensure_schema(g: Graph) -> None:
    """
    Asegura constraints, índices y full-text necesarios. Idempotente.
    - Compatibilidad Neo4j 5.x (DDL) y 4.x (procedimiento full-text).
    - Silencioso si el índice ya existe o el comando no está disponible.
    """
    stmts = [
        # Constraint & índices básicos
        "CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (d:Documento) REQUIRE d.id IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (d:Documento) ON (d.titulo)",
        "CREATE INDEX IF NOT EXISTS FOR (t:Tema)       ON (t.norm)",
        "CREATE INDEX IF NOT EXISTS FOR (e:Entidad)    ON (e.norm)",
        "CREATE INDEX IF NOT EXISTS FOR (a:Articulo)   ON (a.numero)",
    ]

    for q in stmts:
        try:
            g.run(q)
        except Exception:
            # Ignora "already exists" o sintaxis no soportada por la versión
            pass

    # Full-text para Articulos (titulo, texto)
    # 1) Intento DDL (Neo4j 5.x):
    try:
        g.run("CREATE FULLTEXT INDEX ft_articulos IF NOT EXISTS FOR (a:Articulo) ON EACH [a.titulo, a.texto]")
    except Exception:
        # 2) Intento procedimiento (Neo4j 4.x):
        try:
            g.run("CALL db.index.fulltext.createNodeIndex('ft_articulos',['Articulo'],['titulo','texto'])")
        except Exception:
            # Si tampoco existe el procedimiento (edición muy antigua / sin plugin), lo omitimos.
            pass

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
    # Smoke test de conexión
    g.run("RETURN 1").evaluate()
    # Asegura el esquema al momento de conectar (idempotente)
    _ensure_schema(g)
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

def run_cypher(query: str, **params):
    """Ejecuta Cypher con reconexión automática."""
    g = get_graph()
    try:
        return g.run(query, **params)
    except (ConnectionBroken, BrokenWireError):
        g = get_graph(force_new=True)
        return g.run(query, **params)

def evaluate(query: str, **params):
    """Como run().evaluate() pero con reconexión automática."""
    return run_cypher(query, **params).evaluate()
