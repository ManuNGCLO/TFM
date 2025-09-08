# utils/text_to_cypher.py
# -*- coding: utf-8 -*-
"""
Generador NL → Cypher (TFM R46)
- Intents: MENCIONA / TRATA_SOBRE / DEROGA / MODIFICA / ARTÍCULOS DE
- Sinónimos: LOPD→LO 15/1999, LOPDGDD→LO 3/2018, RGPD/GDPR→Reglamento UE 2016/679, AEPD, etc.
- API:
    - gen(q) -> str          (retrocompatible)
    - gen_ex(q, mode) -> (cypher, engine)   [para mostrar el motor en UI]
"""
from __future__ import annotations
import re
import unicodedata
from typing import Tuple

# ----------------------- Normalización y utilidades --------------------------

def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = s.replace("-", " ")  # mantenemos '/' para 3/2018 y 2016/679
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _has_root(q: str, root: str) -> bool:
    """
    Detecta cualquier forma verbal por 'raíz'. Se permite una raíz corta para cubrir
    flexiones como 'modifiquen', 'mencione', 'derogan', etc.
    """
    qn = _norm(q)
    if root == "modific":
        return ("modific" in qn) or ("modifi" in qn) or bool(re.search(r"\bmodific\w*", qn))
    if root == "mencion":
        return ("mencion" in qn) or bool(re.search(r"\bmencion\w*", qn))
    if root == "derog":
        return ("derog" in qn) or bool(re.search(r"\bderog\w*", qn))
    if root == "articul":
        return ("articul" in qn) or bool(re.search(r"\barticul\w*", qn))
    return root in qn

# ----------------------- Sinónimos / términos canónicos ----------------------

_SYNONYMS: list[tuple[str, str]] = [
    # RGPD / GDPR / Reglamento (UE) 2016/679
    (r"\brgpd\b", "reglamento ue 2016 679"),
    (r"\bgdpr\b", "reglamento ue 2016 679"),
    (r"reglamento\s+general\s+de\s+proteccion\s+de\s+datos", "reglamento ue 2016 679"),
    (r"reglamento\s*\(?\s*ue\s*\)?\s*2016\s*/\s*679", "reglamento ue 2016 679"),
    (r"\b2016\s*/\s*679\b|\b2016\s+679\b", "reglamento ue 2016 679"),
    (r"\breglamento\s+ue\s+2016\s*679\b", "reglamento ue 2016 679"),

    # LOPDGDD / LO 3/2018
    (r"\blopdgdd\b", "lo 3 2018"),
    (r"ley\s+organica\s+de\s+proteccion\s+de\s+datos\s+y\s+garantia\s+de\s+derechos\s+digitales", "lo 3 2018"),
    (r"\blo\s*3\s*/\s*2018\b|\bley\s*organica\s*3\s*/\s*2018\b", "lo 3 2018"),

    # LOPD / LO 15/1999
    (r"\blopd\b", "lo 15 1999"),
    (r"ley\s+organica\s+de\s+proteccion\s+de\s+datos(?!\s+y)", "lo 15 1999"),
    (r"\blo\s*15\s*/\s*1999\b|\bley\s*organica\s*15\s*/\s*1999\b", "lo 15 1999"),

    # AEPD
    (r"\baepd\b", "aepd"),
    (r"agencia\s+espanola\s+de\s+proteccion\s+de\s+datos", "aepd"),

    # Memoria AEPD 2024 (atajo útil)
    (r"\bmemoria\s+(?:anual\s+)?aepd\s*2024\b|\bmemoria\s*2024\b|\baepd\s*2024\b", "memoria 2024"),
]

def _apply_synonyms(qn: str) -> str | None:
    for pat, canon in _SYNONYMS:
        if re.search(pat, qn):
            return canon
    return None

def _doc_term_from_question(q: str) -> str:
    qn = _norm(q)
    syn = _apply_synonyms(qn)
    if syn:
        return syn
    m = re.search(r'"([^\\"]+)"', q)
    if m:
        return _norm(m.group(1))
    if "memoria 2024" in qn:
        return "memoria 2024"
    return ""

# --------------------------- Generación de Cypher (reglas) -------------------

def _rules(q: str) -> str:
    qn = _norm(q)
    term = _doc_term_from_question(q)
    id_like = term.replace(" ", "-")

    # 1) ¿Qué documentos mencionan X?
    if _has_root(q, "mencion"):
        # RGPD/GDPR
        if term == "reglamento ue 2016 679" or any(k in qn for k in ["rgpd", "gdpr", "2016/679", "2016 679"]):
            return ("""
            // Documentos que mencionan RGPD (por documento o por entidad)
            MATCH (d:Documento)-[:MENCIONA_DOC]->(x:Documento)
            WHERE toLower(x.id) CONTAINS 'reglamento-ue-2016-679'
               OR toLower(x.id) CONTAINS 'celex-32016r0679'
               OR toLower(x.id) CONTAINS 'celex-32016r0679-es-txt'
               OR toLower(x.titulo) CONTAINS '2016/679'
               OR toLower(x.titulo) CONTAINS '2016 679'
            RETURN DISTINCT d.id AS documento, d.titulo AS titulo
            UNION
            MATCH (d:Documento)-[:MENCIONA]->(e:Entidad)
            WHERE toLower(coalesce(e.norm, e.nombre)) CONTAINS 'rgpd'
               OR toLower(e.nombre) CONTAINS 'gdpr'
               OR toLower(e.nombre) CONTAINS '2016 679'
            RETURN DISTINCT d.id AS documento, d.titulo AS titulo
            ORDER BY titulo
            """).strip()

        # AEPD (Entidad + ID/Título)
        if term == "aepd" or " aepd" in qn or "agencia espanola de proteccion de datos" in qn:
            return ("""
            // Documentos que mencionan la AEPD (por Entidad o por ID/Título)
            MATCH (d:Documento)-[:MENCIONA]->(e:Entidad)
            WHERE toLower(coalesce(e.norm, e.nombre)) CONTAINS 'aepd'
               OR toLower(coalesce(e.norm, e.nombre)) CONTAINS 'agencia espanola de proteccion de datos'
            RETURN DISTINCT d.id AS documento, d.titulo AS titulo
            UNION
            MATCH (d:Documento)
            WHERE toLower(d.id) CONTAINS 'aepd' OR toLower(d.titulo) CONTAINS 'aepd'
            RETURN DISTINCT d.id AS documento, d.titulo AS titulo
            ORDER BY titulo
            """).strip()

        # Cualquier otro término mapeado a documento/título
        if term:
            return f"""
            // Documentos que mencionan «{term}»
            MATCH (d:Documento)-[:MENCIONA_DOC]->(x:Documento)
            WHERE toLower(x.id)     CONTAINS '{id_like}'
               OR toLower(x.titulo) CONTAINS '{term}'
               OR ( '{term}' = 'lo 3 2018'   AND toLower(x.id) CONTAINS 'boe-a-2018-16673')
               OR ( '{term}' = 'lo 15 1999'  AND toLower(x.id) CONTAINS 'boe-a-1999-23750')
            RETURN DISTINCT d.id AS documento, d.titulo AS titulo
            ORDER BY titulo
            """.strip()

    # 2) ¿Qué modifica … ?
    if _has_root(q, "modific"):
        if term:
            return f"""
            // ¿Qué modifica «{term}»?
            MATCH (src:Documento)-[:MODIFICA]->(dst:Documento)
            WHERE toLower(src.id)     CONTAINS '{id_like}'
               OR toLower(src.titulo)  CONTAINS '{term}'
               OR ( '{term}' = 'lo 3 2018' AND toLower(src.id) CONTAINS 'boe-a-2018-16673')
            RETURN src.titulo AS documento, 'MODIFICA' AS relacion, dst.titulo AS sobre
            ORDER BY documento
            LIMIT 100
            """.strip()
        # Sin término: lista general de documentos que modifican algo
        return """
        // Documentos que MODIFICAN otras normas (lista general)
        MATCH (src:Documento)-[:MODIFICA]->(:Documento)
        RETURN DISTINCT src.id AS id, src.titulo AS titulo
        ORDER BY titulo
        LIMIT 200
        """.strip()

    # 3) ¿Qué deroga … ? (saliente y entrante)
    if _has_root(q, "derog"):
        if term:
            return f"""
            // DEROGA respecto a «{term}»
            WITH '{id_like}' AS id_like, '{term}' AS term
            MATCH (src:Documento)-[:DEROGA]->(dst:Documento)
            WITH src, dst,
                 (
                   toLower(src.id) CONTAINS id_like OR
                   toLower(src.titulo) CONTAINS term OR
                   (term='lo 3 2018' AND toLower(src.id) CONTAINS 'boe-a-2018-16673')
                 ) AS is_src,
                 (
                   toLower(dst.id) CONTAINS id_like OR
                   toLower(dst.titulo) CONTAINS term OR
                   (term='lo 15 1999' AND toLower(dst.id) CONTAINS 'boe-a-1999-23750')
                 ) AS is_dst
            WHERE is_src OR is_dst
            RETURN src.titulo AS documento,
                   CASE WHEN is_src THEN 'DEROGA' ELSE 'DEROGA A' END AS relacion,
                   dst.titulo AS sobre
            ORDER BY documento
            LIMIT 100
            """.strip()
        # Sin término: lista general de documentos que derogan algo
        return """
        // Documentos que DEROGAN otras normas (lista general)
        MATCH (src:Documento)-[:DEROGA]->(:Documento)
        RETURN DISTINCT src.id AS id, src.titulo AS titulo
        ORDER BY titulo
        LIMIT 200
        """.strip()

    # 4) Artículos de <documento>
    if _has_root(q, "articul") and term:
        return f"""
        // Artículos de «{term}»
        MATCH (d:Documento)-[:TIENE_ARTICULO]->(a:Articulo)
        WHERE toLower(d.id)     CONTAINS '{id_like}'
           OR toLower(d.titulo) CONTAINS '{term}'
           OR ( '{term}' = 'lo 3 2018'  AND toLower(d.id) CONTAINS 'boe-a-2018-16673')
           OR ( '{term}' = 'lo 15 1999' AND toLower(d.id) CONTAINS 'boe-a-1999-23750')
           OR ( '{term}' = 'lo 15 1999' AND toLower(d.id) CONTAINS 'boe-a-1999-23750-consolidado')
        RETURN d.id AS doc, a.id AS art_id, a.numero AS numero, a.titulo AS titulo
        ORDER BY toInteger(coalesce(a.numero,'0')) ASC, a.titulo
        LIMIT 500
        """.strip()

    # 5) Documentos que tratan sobre Protección de Datos (opcional: vigentes)
    if "proteccion de datos" in qn or "protección de datos" in qn or "proteccion datos" in qn:
        only_vig = "vigent" in qn
        return ("""
        // Documentos que tratan sobre Protección de Datos
        MATCH (d:Documento)-[:TRATA_SOBRE]->(t:Tema)
        WHERE (toLower(coalesce(t.norm,t.nombre)) CONTAINS 'proteccion' AND
               toLower(coalesce(t.norm,t.nombre)) CONTAINS 'datos')
        """ + ("AND coalesce(d.vigente,true) = true\n" if only_vig else "") + """
        RETURN d.id AS id, d.titulo AS titulo
        ORDER BY titulo
        LIMIT 200
        """).strip()

    # Fallback
    return """
    // No reconocí el patrón de la pregunta.
    // Ejemplos:
    // - '¿Qué documentos mencionan RGPD?'
    // - '¿Qué documentos mencionan la Ley Orgánica 3/2018?'
    // - '¿Qué documentos tratan sobre Protección de Datos vigentes?'
    // - '¿Qué deroga LO 3/2018?' / '¿Quién deroga LO 15/1999?'
    // - '¿Qué modifica LO 3/2018?'
    // - '¿Qué artículos contiene LO 3/2018?'
    RETURN 'Pregunta no reconocida' AS aviso
    """.strip()

# --------------------------- API pública ---------------------

def gen(q: str) -> str:
    """Retrocompatible: devuelve solo el Cypher (modo reglas)."""
    return _rules(q)

def gen_ex(q: str, mode: str = "auto") -> Tuple[str, str]:
    """
    Devuelve (cypher, engine). 'mode' se mantiene por compatibilidad futura.
    En esta implementación siempre usamos reglas → engine='rules'.
    """
    cy = _rules(q)
    return cy, "rules"
