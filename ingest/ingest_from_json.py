# app/ingest/ingest_from_json.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple

# --- normalización (con fallback para no romper si falta el módulo) ---
try:
    from ingest.normalization import canonical, slugify  # proyecto
except Exception:
    import re, unicodedata
    _SPACES = re.compile(r"\s+")
    def canonical(s: str | None) -> str:
        if not s:
            return ""
        s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
        s = s.strip().lower()
        return _SPACES.sub(" ", s)
    def slugify(s: str | None) -> str:
        s = canonical(s)
        s = re.sub(r"[^a-z0-9\-_/\. ]+", "", s)
        s = s.replace("/", "-").replace(" ", "-").replace("_", "-")
        s = re.sub(r"-{2,}", "-", s).strip("-")
        return s or "documento"

def _norm_list(xs):
    if not xs:
        return []
    return [canonical(x) for x in xs if x]

def _ensure_doc_id(d: Dict[str, Any]) -> str:
    """
    Prioridad del id:
      1) d['id'] explícito
      2) slug del título
      3) 'documento'
    """
    raw_id = (d.get("id") or "").strip()
    if raw_id:
        return slugify(canonical(raw_id))
    title = (d.get("titulo") or "").strip()
    if title:
        return slugify(canonical(title))
    return "documento"

def upsert_document(graph, d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inserta/actualiza Documento y (si vienen) Articulos, Temas, Entidades y Relaciones.
    JSON mínimo esperado:
      { titulo, tipo?, fecha?, vigente?, fuente?, articulos?, temas?, entidades?, relaciones? }
    Devuelve: { id, titulo, num_articulos }
    """
    # --- normalización base
    doc_id    = _ensure_doc_id(d)
    titulo    = (d.get("titulo") or "documento").strip()
    tipo      = (d.get("tipo") or "Documento").strip()
    fuente    = (d.get("fuente") or "").strip()
    fecha     = (d.get("fecha") or "").strip()
    vigente   = d.get("vigente")
    temas     = _norm_list(d.get("temas"))
    entidades = _norm_list(d.get("entidades"))

    # --- upsert del documento ---
    q_doc = """
    MERGE (doc:Documento {id:$id})
      ON CREATE SET doc.titulo=$titulo, doc.tipo=$tipo, doc.fuente=$fuente, doc.fecha=$fecha, doc.vigente=$vigente
      ON MATCH  SET doc.titulo=$titulo, doc.tipo=$tipo, doc.fuente=$fuente, doc.fecha=$fecha, doc.vigente=$vigente
    RETURN doc.id AS id
    """
    graph.run(q_doc, id=doc_id, titulo=titulo, tipo=tipo, fuente=fuente, fecha=fecha, vigente=vigente)

    # --- temas ---
    if temas:
        graph.run("""
            MATCH (d:Documento {id:$id})
            UNWIND $temas AS t
            MERGE (tt:Tema {nombre:t})
            SET tt.norm = t
            MERGE (d)-[:TRATA_SOBRE]->(tt)
        """, id=doc_id, temas=temas)

    # --- entidades mencionadas ---
    if entidades:
        graph.run("""
            MATCH (d:Documento {id:$id})
            UNWIND $ents AS e
            MERGE (en:Entidad {nombre:e})
            SET en.norm = e
            MERGE (d)-[:MENCIONA]->(en)
        """, id=doc_id, ents=entidades)

    # --- artículos ---
    arts = d.get("articulos") or []
    if arts:
        cooked = []
        for a in arts:
            if not a:
                continue
            numero = a.get("numero")
            aid = a.get("id") or f"{doc_id}-art-{numero or ''}"
            cooked.append({
                "id": aid,
                "numero": numero,
                "titulo": a.get("titulo"),
                "texto": a.get("texto")
            })
        if cooked:
            graph.run("""
                UNWIND $arts AS a
                MERGE (art:Articulo {id:a.id})
                  ON CREATE SET art.numero=a.numero, art.titulo=a.titulo, art.texto=a.texto
                  ON MATCH  SET art.numero=a.numero, art.titulo=a.titulo, art.texto=a.texto
            """, arts=cooked)
            graph.run("""
                MATCH (d:Documento {id:$id})
                UNWIND $arts AS a
                MATCH (art:Articulo {id:a.id})
                MERGE (d)-[:TIENE_ARTICULO]->(art)
            """, id=doc_id, arts=cooked)

    # --- documento->documento (relaciones) ---
    rels = d.get("relaciones") or []
    if rels:
        cooked = []
        for r in rels:
            if not r:
                continue
            rtipo   = (r.get("tipo") or "MENCIONA").upper()
            obj_tit = (r.get("documento") or r.get("obj_titulo") or "").strip()
            obj_id  = slugify(canonical(r.get("obj_id") or obj_tit))
            if not obj_id:
                continue
            cooked.append({"tipo": rtipo, "obj_id": obj_id, "obj_titulo": obj_tit})
        if cooked:
            graph.run("""
                MATCH (src:Documento {id:$src_id})
                UNWIND $rels AS r
                MERGE (dst:Documento {id:r.obj_id})
                  ON CREATE SET dst.titulo = r.obj_titulo
                FOREACH (_ IN CASE WHEN r.tipo='MODIFICA' THEN [1] ELSE [] END |
                  MERGE (src)-[:MODIFICA]->(dst)
                )
                FOREACH (_ IN CASE WHEN r.tipo='DEROGA' THEN [1] ELSE [] END |
                  MERGE (src)-[:DEROGA]->(dst)
                )
                FOREACH (_ IN CASE WHEN r.tipo='MENCIONA' THEN [1] ELSE [] END |
                  MERGE (src)-[:MENCIONA_DOC]->(dst)
                )
            """, src_id=doc_id, rels=cooked)

    # --- métricas para UI ---
    counts = graph.run("""
        MATCH (d:Documento {id:$id})
        OPTIONAL MATCH (d)-[:TIENE_ARTICULO]->(a:Articulo)
        RETURN count(a) AS num_articulos
    """, id=doc_id).data()[0]
    return {"id": doc_id, "titulo": titulo, "num_articulos": counts.get("num_articulos", 0)}

# --- Compat: mantener import antiguo ---
def ingest_json(graph, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Compatibilidad con `from app.ingest.ingest_from_json import ingest_json`."""
    return upsert_document(graph, payload)

# --- Utilidades de reseteo (opcional) ---
def wipe_database_and_constraints(graph) -> None:
    graph.run("MATCH (n) DETACH DELETE n")
    graph.run("DROP CONSTRAINT doc_id IF EXISTS;")
    graph.run("DROP CONSTRAINT art_id IF EXISTS;")
    graph.run("DROP CONSTRAINT tema_nombre IF EXISTS;")
    graph.run("DROP CONSTRAINT ent_nombre IF EXISTS;")
    graph.run("CREATE CONSTRAINT doc_id      IF NOT EXISTS FOR (d:Documento) REQUIRE d.id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT art_id      IF NOT EXISTS FOR (a:Articulo)  REQUIRE a.id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT tema_nombre IF NOT EXISTS FOR (t:Tema)      REQUIRE t.nombre IS UNIQUE;")
    graph.run("CREATE CONSTRAINT ent_nombre  IF NOT EXISTS FOR (e:Entidad)   REQUIRE e.nombre IS UNIQUE;")
