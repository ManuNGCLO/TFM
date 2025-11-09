# utils/text_to_cypher.py
# -*- coding: utf-8 -*-
"""
TFM R46 – Generador NL/Text → Cypher (versión final mejorada para Neo4j Aura)
-----------------------------------------------------------------------------

✔ Crea relaciones semánticas automáticamente:
  - TRATA_SOBRE → :Tema
  - MENCIONA_DOC → :Documento
  - DEROGA / MODIFICA → :Documento
  - MENCIONA → :Entidad (AEPD, RGPD, GDPR, Reglamento UE 2016/679)
✔ Compatible con Neo4j Aura (una sola query por transacción)
✔ Evita reuso de alias (t0, x0, e0, e1)
✔ Incluye motor NL2Cypher por reglas + mejoras de precisión semántica
"""

from __future__ import annotations
import re
import unicodedata
from typing import List, Tuple, Dict, Optional

# ==============================================================
# 🔧 Utilidades
# ==============================================================

def _norm(s: str) -> str:
    """Normaliza texto: minúsculas, sin tildes ni dobles espacios."""
    if not s:
        return ""
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = s.replace("-", " ")
    return re.sub(r"\s+", " ", s).strip()

def _esc(s: str) -> str:
    """Escapa comillas simples para Cypher."""
    return s.replace("'", "\\'") if s else ""

def _has_root(q: str, root: str) -> bool:
    """Detecta raíces verbales para intents."""
    qn = _norm(q)
    if root == "modific":
        return "modific" in qn or "modifi" in qn or bool(re.search(r"\bmodific\w*", qn))
    if root == "mencion":
        return "mencion" in qn or bool(re.search(r"\bmencion\w*", qn))
    if root == "derog":
        return "derog" in qn or bool(re.search(r"\bderog\w*", qn))
    if root == "articul":
        return "articul" in qn or bool(re.search(r"\barticul\w*", qn))
    if root == "trat":
        return any(x in qn for x in ["trata", "tema", "materia"])
    return root in qn


# ==============================================================
# 🧩 Sinónimos y normalización
# ==============================================================

_SYNONYMS = [
    (r"\brgpd\b", "reglamento ue 2016 679"),
    (r"\bgdpr\b", "reglamento ue 2016 679"),
    (r"reglamento\s+(ue\s*)?2016\s*/\s*679", "reglamento ue 2016 679"),
    (r"\blo\s*3\s*/\s*2018\b", "lo 3 2018"),
    (r"\blo\s*15\s*/\s*1999\b", "lo 15 1999"),
    (r"\baepd\b", "aepd"),
]

def _apply_synonyms(qn: str) -> Optional[str]:
    for pat, canon in _SYNONYMS:
        if re.search(pat, qn):
            return canon
    return None

def _doc_term_from_question(q: str) -> str:
    qn = _norm(q)
    syn = _apply_synonyms(qn)
    if syn:
        return syn
    m = re.search(r'"([^"]+)"', q)
    if m:
        return _norm(m.group(1))
    return ""


# ==============================================================
# 🧭 Patrones de detección
# ==============================================================

_PAT_BOE_ID = re.compile(r"\bboe-[a-z]-\d{4}-\d{4,6}(?:-consolidado)?\b", re.I)
_PAT_CELEX = re.compile(r"\bcelex[_-]?\d{4}[a-z]\d{4}[a-z]?(?:[_-]es[_-]txt)?\b", re.I)
_PAT_LO = re.compile(r"\blo\s*(\d{1,3})\s*/\s*(\d{4})\b", re.I)
_PAT_RD = re.compile(r"\brd\s*(\d{1,4})\s*/\s*(\d{4})\b", re.I)
_PAT_LEY = re.compile(r"\bley\s*(\d{1,4})\s*/\s*(\d{4})\b", re.I)
_PAT_NUMSLASH = re.compile(r"\b(\d{1,4})\s*/\s*(\d{4})\b", re.I)

_TOPICS = {
    "Protección de Datos": [r"proteccion\s+de\s+datos", r"\brgpd\b", r"\bgdpr\b"],
    "Derechos Digitales": [r"derechos\s+digitales"],
    "Transparencia": [r"\btransparencia\b"],
}


# ==============================================================
# 🔍 Detección desde texto de artículos
# ==============================================================

def _find_doc_refs_in_text(text: str) -> Dict[str, List[str]]:
    refs = {"boe": [], "celex": [], "lo": [], "rd": [], "ley": [], "generic": []}
    for m in _PAT_BOE_ID.finditer(text): refs["boe"].append(m.group(0).lower())
    for m in _PAT_CELEX.finditer(text): refs["celex"].append(m.group(0).lower())
    for m in _PAT_LO.finditer(text): refs["lo"].append(f"lo {m.group(1)} {m.group(2)}")
    for m in _PAT_RD.finditer(text): refs["rd"].append(f"rd {m.group(1)} {m.group(2)}")
    for m in _PAT_LEY.finditer(text): refs["ley"].append(f"ley {m.group(1)} {m.group(2)}")
    for m in _PAT_NUMSLASH.finditer(text):
        n, y = m.group(1), m.group(2)
        if f"{n}/{y}" != "2016/679":
            refs["generic"].append(f"{n} {y}")
    return refs

def _detect_actions(text: str) -> Dict[str, bool]:
    t = _norm(text)
    return {
        "deroga": "derog" in t,
        "modifica": "modific" in t or "modifi" in t,
        "menciona": "mencion" in t,
        "trata": any(re.search(p, t) for pats in _TOPICS.values() for p in pats),
    }

def _find_topics(text: str) -> List[str]:
    t = _norm(text)
    return [topic for topic, pats in _TOPICS.items() if any(re.search(p, t) for p in pats)]


# ==============================================================
# 🧱 Construcción de Cypher para inferencia semántica
# ==============================================================

def _merge_doc_by_id(doc_id: str) -> str:
    return f"MERGE (d:Documento {{id:'{_esc(doc_id)}'}})"

def _infer_and_build(article_text: str, doc_id: str) -> str:
    """Construye el bloque Cypher inferido desde un artículo (modo Aura)."""
    text = article_text or ""
    tnorm = _norm(text)
    actions = _detect_actions(text)
    refs = _find_doc_refs_in_text(text)
    topics = _find_topics(text)

    cy: List[str] = []
    cy.append(_merge_doc_by_id(doc_id))

    # 1️⃣ TÓPICOS
    for idx, topic in enumerate(topics):
        alias_t = f"t{idx}"
        cy.append(f"MERGE ({alias_t}:Tema {{nombre:'{_esc(topic)}'}})")
        cy.append(f"MERGE (d)-[:TRATA_SOBRE]->({alias_t})")

    # 2️⃣ ENTIDADES
    ent_idx = 0
    if re.search(r"\baepd\b|agencia\s+espanola\s+de\s+proteccion\s+de\s+datos", tnorm):
        cy.append(f"MERGE (e{ent_idx}:Entidad {{nombre:'AEPD'}})")
        cy.append(f"MERGE (d)-[:MENCIONA]->(e{ent_idx})")
        ent_idx += 1
    if re.search(r"\brgpd\b|\bgdpr\b|reglamento\s+(ue\s*)?2016\s*/\s*679", tnorm):
        cy.append(f"MERGE (e{ent_idx}:Entidad {{nombre:'RGPD'}})")
        cy.append(f"MERGE (d)-[:MENCIONA]->(e{ent_idx})")
        ent_idx += 1

    # 3️⃣ REFERENCIAS A OTROS DOCUMENTOS
    doc_targets: List[str] = refs["boe"] + refs["celex"] + refs["lo"] + refs["rd"] + refs["ley"]
    for g in refs["generic"]:
        if g != "2016 679":
            doc_targets.append(g)

    if actions["menciona"] and not doc_targets:
        if "lo 3 2018" in tnorm: doc_targets.append("lo 3 2018")
        if "lo 15 1999" in tnorm: doc_targets.append("lo 15 1999")
        if "2016 679" in tnorm: doc_targets.append("reglamento ue 2016 679")

    rel_type = "DEROGA" if actions["deroga"] else "MODIFICA" if actions["modifica"] else None

    for idx, tgt in enumerate(doc_targets):
        alias_dst = f"x{idx}"
        cy.append(f"MERGE ({alias_dst}:Documento {{id:'{_esc(tgt)}'}})")
        cy.append(f"ON CREATE SET {alias_dst}.titulo = coalesce({alias_dst}.titulo, '{_esc(tgt)}')")
        if rel_type:
            cy.append(f"MERGE (d)-[:{rel_type}]->({alias_dst})")
        else:
            cy.append(f"MERGE (d)-[:MENCIONA_DOC]->({alias_dst})")

    cy.append("RETURN 'Relaciones generadas en Neo4j Aura' AS status;")

    result = "\n".join(cy)
    stripped = re.sub(r"\s+", " ", result)
    if not any(k in stripped for k in ["TRATA_SOBRE", "MENCIONA_DOC", "DEROGA", "MODIFICA", "MENCIONA]->(e"]):
        return ""
    return result


# ==============================================================
# 🔎 Motor NL → Cypher (modo lectura mejorado)
# ==============================================================

def _rules(q: str) -> str:
    qn = _norm(q)
    term = _doc_term_from_question(q)
    id_like = term.replace(" ", "-")

    # --- NUEVOS PATRONES SEMÁNTICOS ---
    if "rgpd" in qn or "gdpr" in qn or "reglamento 2016 679" in qn:
        return """
MATCH (d:Documento)-[:MENCIONA]->(e:Entidad)
WHERE toLower(e.nombre) CONTAINS 'rgpd' OR toLower(coalesce(e.norm,e.nombre)) CONTAINS '2016/679'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo
""".strip()

    if "aepd" in qn or "agencia espanola de proteccion de datos" in qn:
        return """
MATCH (d:Documento)-[:MENCIONA]->(e:Entidad)
WHERE toLower(e.nombre) CONTAINS 'aepd'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo
""".strip()

    if "vigent" in qn or "actual" in qn:
        return """
MATCH (d:Documento)
WHERE coalesce(d.vigente,true) = true
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo
""".strip()

    if "proteccion de datos" in qn or "protección de datos" in qn:
        return """
MATCH (d:Documento)-[:TRATA_SOBRE]->(t:Tema)
WHERE toLower(t.nombre) CONTAINS 'proteccion' AND toLower(t.nombre) CONTAINS 'datos'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo
""".strip()

    # --- Reglas estándar originales (con mejoras) ---
    if _has_root(q, "mencion"):
        return f"""
MATCH (d:Documento)-[:MENCIONA_DOC|MENCIONA]->(x)
WHERE toLower(x.id) CONTAINS '{id_like}'
   OR toLower(coalesce(x.titulo, x.nombre)) CONTAINS '{term}'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo
""".strip()

    if _has_root(q, "modific"):
        return """
MATCH (a:Documento)-[:MODIFICA]->(b:Documento)
RETURN a.id AS origen, b.id AS destino, 'MODIFICA' AS relacion
ORDER BY origen
""".strip()

    if _has_root(q, "derog"):
        return """
MATCH (a:Documento)-[:DEROGA]->(b:Documento)
RETURN a.id AS origen, b.id AS destino, 'DEROGA' AS relacion
ORDER BY origen
""".strip()

    if _has_root(q, "articul"):
        return f"""
MATCH (d:Documento)-[:TIENE_ARTICULO]->(a:Articulo)
WHERE toLower(d.id) CONTAINS '{id_like}' OR toLower(d.titulo) CONTAINS '{term}'
RETURN d.id AS doc, a.numero AS numero, a.titulo AS titulo
ORDER BY toInteger(coalesce(a.numero,'0')) ASC
""".strip()

    if "proteccion de datos" in qn or _has_root(q, "trat"):
        return """
MATCH (d:Documento)-[:TRATA_SOBRE]->(t:Tema)
WHERE toLower(t.nombre) CONTAINS 'proteccion' AND toLower(t.nombre) CONTAINS 'datos'
RETURN d.id AS id, d.titulo AS titulo
ORDER BY titulo
""".strip()

    return "RETURN 'Pregunta no reconocida' AS aviso"


# ==============================================================
# 🧠 API Pública
# ==============================================================

def gen(q: str, doc_id: str | None = None) -> str:
    if doc_id:
        return _infer_and_build(q, doc_id)
    return _rules(q)

def gen_ex(q: str, mode: str = "auto") -> Tuple[str, str]:
    cy = _rules(q)
    return cy, "rules"


# ==============================================================
# 🧪 Test local
# ==============================================================

if __name__ == "__main__":
    sample = """
    Esta norma deroga la Ley 2/1995, modifica la LO 3/2018,
    trata sobre protección de datos personales y menciona la AEPD y el RGPD.
    """
    print(gen(sample, doc_id="boe-a-2020-demo"))
