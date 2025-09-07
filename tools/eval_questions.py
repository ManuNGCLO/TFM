# -*- coding: utf-8 -*-
"""
tools/eval_questions.py

Evalúa NL→Cypher contra un conjunto de preguntas con ground-truth.
Genera:
  - results/results.jsonl  (una línea por (pregunta x motor))
  - results/summary.csv    (resumen por motor)

Uso:
  python tools/eval_questions.py --engines rules rules_fb
  python tools/eval_questions.py --engines rules rules_fb gpt gpt_fb --model gpt-4o-mini
  python tools/eval_questions.py --limit 50
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st  # para leer secrets (OPENAI_API_KEY/MODEL) si estuvieran

# Rutas base
ROOT = Path(__file__).resolve().parents[1]
DATA_CSV = ROOT / "data" / "questions.csv"
RESULTS_DIR = ROOT / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_JSONL = RESULTS_DIR / "results.jsonl"
SUMMARY_CSV = RESULTS_DIR / "summary.csv"

# Reutilizamos la conexión y ejecución de tu app
sys.path.append(str(ROOT))  # añade raíz del proyecto al PYTHONPATH
from utils.graph_client import get_graph, run_cypher  # noqa
from utils.text_to_cypher import gen as rules_gen  # noqa


# ------------------------ Utilidades de Cypher -------------------------------
def is_safe_cypher(cy: str) -> bool:
    """Bloquea operaciones de escritura o potencialmente peligrosas."""
    bad = [" create ", " merge ", " delete ", " set ", " detach ", " drop ", " load csv", " call db.", " call apoc.create"]
    low = f" {cy.lower()} "
    return not any(tok in low for tok in bad)

def _normalize_return(cy: str) -> str:
    """Si se retorna el nodo Documento directamente, normaliza a id/titulo cuando se detecta alias."""
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

def _fix_label_props_to_alias(cy: str) -> str:
    """Cambia 'Tema.nombre' → 't.nombre' si existe MATCH (t:Tema)."""
    try:
        label2alias = {}
        for m in re.finditer(r"\(\s*([a-zA-Z][\w]*)\s*:\s*([A-Z][\w]*)\s*\)", cy):
            alias, label = m.group(1), m.group(2)
            label2alias.setdefault(label, alias)
        def repl(m):
            label, prop = m.group(1), m.group(2)
            alias = label2alias.get(label)
            return f"{alias}.{prop}" if alias else m.group(0)
        return re.sub(r"\b([A-Z][A-Za-z0-9_]*)\.(\w+)\b", repl, cy)
    except Exception:
        return cy

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

def _fallback_from_question(q: str) -> str | None:
    """Fallbacks clave como en la UI."""
    qn = q.lower().strip()
    # Modifican LO 3/2018 y tratan sobre X
    if ("modific" in qn) and any(k in qn for k in ["lo 3/2018","lo 3 2018","ley organica 3/2018","ley orgánica 3/2018"]):
        m = re.search(r"sobre\s+['\"“”‘’«»]?([^'\"“”‘’«»]+)", qn)
        term = (m.group(1).strip() if m else "consentimiento")
        return _build_theme_fallback_fulltext(term, keep_modifica=True, keep_vigente=False)
    # Vigentes + derogan + rgpd
    if ("vigen" in qn) and ("derog" in qn) and any(t in qn for t in ["rgpd","2016/679","reglamento 2016/679","gdpr"]):
        return """
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
""".strip()
    return None


# ------------------------ GPT opcional (few-shot + prompt estricto) ----------
FEW_SHOTS = [
    # 1
    ("¿Qué documentos mencionan RGPD?",
     """MATCH (d:Documento)-[:MENCIONA]->(e:Entidad)
WHERE toLower(coalesce(e.norm,e.nombre)) CONTAINS 'rgpd'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo"""),
    # 2
    ("Documentos que mencionan la Ley Orgánica 3/2018",
     """MATCH (d:Documento)-[:MENCIONA_DOC]->(x:Documento)
WHERE toLower(x.id) CONTAINS 'boe-a-2018-16673'
   OR toLower(x.titulo) CONTAINS 'lo 3 2018'
   OR toLower(x.titulo) CONTAINS 'ley organica 3/2018'
   OR toLower(x.titulo) CONTAINS 'ley orgánica 3/2018'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo"""),
    # 3
    ("Documentos que tratan sobre Protección de Datos vigentes",
     """MATCH (d:Documento)-[:TRATA_SOBRE]->(t:Tema)
WHERE coalesce(d.vigente,true) = true
  AND toLower(coalesce(t.norm,t.nombre)) CONTAINS 'protección de datos'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo"""),
    # 4
    ("¿Qué deroga LO 3/2018?",
     """MATCH (d:Documento)-[:DEROGA]->(d2:Documento)
WHERE toLower(d2.id) CONTAINS 'boe-a-2018-16673'
   OR toLower(d2.titulo) CONTAINS 'lo 3 2018'
   OR toLower(d2.titulo) CONTAINS 'ley organica 3/2018'
   OR toLower(d2.titulo) CONTAINS 'ley orgánica 3/2018'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo"""),
    # 5
    ("Documentos que modifiquen LO 3/2018 y traten sobre consentimiento",
     """MATCH (d:Documento)-[:MODIFICA]->(d2:Documento)
WHERE toLower(d2.id) CONTAINS 'boe-a-2018-16673'
   OR toLower(d2.titulo) CONTAINS 'lo 3 2018'
   OR toLower(d2.titulo) CONTAINS 'ley organica 3/2018'
   OR toLower(d2.titulo) CONTAINS 'ley orgánica 3/2018'
WITH DISTINCT d
MATCH (d)-[:TIENE_ARTICULO]->(a:Articulo)
WHERE toLower(coalesce(a.titulo,'')) CONTAINS 'consentimiento'
   OR toLower(coalesce(a.texto,'')) CONTAINS 'consentimiento'
RETURN DISTINCT d.id AS id, d.titulo AS titulo
ORDER BY titulo"""),
]

SCHEMA_TEXT = f"""
Eres un generador de Cypher SOLO DE LECTURA. Esquema:
(:Documento {{id, titulo, tipo, fecha, vigente}})
  -[:TIENE_ARTICULO]-> (:Articulo {{id, doc?, numero, titulo, texto}})
(:Documento)-[:TRATA_SOBRE]->(:Tema {{nombre, norm?}})
(:Documento)-[:MENCIONA]->(:Entidad {{nombre, norm?}})
(:Documento)-[:MENCIONA_DOC]->(:Documento)
(:Documento)-[:DEROGA]->(:Documento)
(:Documento)-[:MODIFICA]->(:Documento)
Reglas:
- SOLO MATCH/OPTIONAL MATCH/WHERE/RETURN/ORDER/LIMIT. NADA de CREATE/MERGE/DELETE/SET.
- Declara SIEMPRE un alias principal `d` para (:Documento) cuyas filas quieres listar.
- Devuelve EXACTAMENTE:
  RETURN DISTINCT d.id AS id, d.titulo AS titulo
  (y si necesitas, ORDER BY titulo)
- Prohibido devolver el nodo completo `d`. Devuelve SIEMPRE las propiedades pedidas.
- Responde SOLO un bloque ```cypher ...```.
- Si no puedes, responde exactamente: FALLBACK

Ejemplos (NL → Cypher):
""" + "\n\n".join(
    [f"- {q}\n```cypher\n{c}\n```" for q, c in FEW_SHOTS]
)

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

def gpt_nl2cypher(question: str, model_hint: str | None = None) -> str:
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
    except Exception:
        return "FALLBACK"
    m = re.search(r"```(?:cypher)?\s*([\s\S]+?)```", content, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    looks_cypher = any(tok in content.lower() for tok in ["match", "return"])
    return content.strip() if looks_cypher else "FALLBACK"


# ------------------------ Métricas ------------------------------------------
def extract_ids(df: pd.DataFrame) -> set[str]:
    """Devuelve conjunto de IDs robusto contra Node/dict/obj/str."""
    if df is None or df.empty:
        return set()

    cols = list(df.columns)
    low = [c.lower() for c in cols]

    # 1) id explícito
    if "id" in low:
        c = cols[low.index("id")]
        return set(map(lambda x: str(x).strip(), df[c].dropna().astype(str).tolist()))

    def _try_get_id(v):
        # dict plano
        if isinstance(v, dict):
            vid = v.get("id")
            if vid is not None:
                return str(vid)
        # mapping-like (tiene .get)
        if hasattr(v, "get"):
            try:
                vid = v.get("id", None)
                if vid is not None:
                    return str(vid)
            except Exception:
                pass
        # indexable tipo mapping: v['id'] (py2neo Node soporta __getitem__)
        try:
            vid = v["id"]  # noqa
            if vid is not None:
                return str(vid)
        except Exception:
            pass
        # a veces props en _properties
        try:
            props = getattr(v, "_properties", None)
            if isinstance(props, dict) and "id" in props:
                return str(props["id"])
        except Exception:
            pass
        # regex sobre str(v)
        try:
            s = str(v)
            m = re.search(r"(?:\bid\b\s*[:=]\s*['\"]?)([\w\-./]+)", s, flags=re.IGNORECASE)
            if m:
                return m.group(1)
        except Exception:
            pass
        return None

    # 2) barrido por columnas object buscando 'id' en celdas
    for c in cols:
        ser = df[c]
        if ser.dtype != "object":
            continue
        vals = ser.dropna().tolist()
        if not vals:
            continue
        got = []
        for v in vals:
            vid = _try_get_id(v)
            if vid:
                got.append(vid)
        if got:
            return set(got)

    # 3) fallback a primera columna de texto
    for c in cols:
        if df[c].dtype == "object":
            return set(map(lambda x: str(x).strip(), df[c].dropna().astype(str).tolist()))

    return set()

def prf(pred: set[str], gold: set[str]) -> tuple[float, float, float]:
    if not gold and not pred:
        return 1.0, 1.0, 1.0
    if not gold:
        return 0.0, 1.0 if pred else 1.0, 0.0
    tp = len(pred & gold)
    p = tp / (len(pred) or 1)
    r = tp / (len(gold) or 1)
    f1 = 0.0 if (p + r == 0) else 2 * p * r / (p + r)
    return p, r, f1


# ------------------------ Refuerzo retorno GPT -------------------------------
def _enforce_document_return(cy: str) -> str:
    """Si hay exactamente un alias de :Documento, fuerza RETURN DISTINCT <alias>.id AS id, <alias>.titulo AS titulo."""
    try:
        aliases = re.findall(r"\(\s*([a-zA-Z]\w*)\s*:\s*Documento\b", cy)
        aliases = [a for a in aliases if a]
        aliases = list(dict.fromkeys(aliases))  # únicos
        if len(aliases) != 1:
            return cy
        a = aliases[0]
        # Sustituir RETURN por el estándar y ORDER BY por titulo
        cy2 = re.sub(r"(?is)\bRETURN\b[\s\S]*?(?=$)", f"RETURN DISTINCT {a}.id AS id, {a}.titulo AS titulo", cy.strip())
        if re.search(r"(?i)\bORDER\s+BY\b", cy2):
            cy2 = re.sub(r"(?i)\bORDER\s+BY\b[\s\S]*?(?=$)", "ORDER BY titulo", cy2)
        else:
            cy2 += "\nORDER BY titulo"
        return cy2
    except Exception:
        return cy


# ------------------------ Rescue policy para gpt_fb --------------------------
def _quick_count(cy: str) -> int:
    """Ejecuta y devuelve len(df). Si falla, -1."""
    try:
        df = run_cypher(cy).to_data_frame()
        return 0 if df is None else len(df)
    except Exception:
        return -1


# ------------------------ Pipeline de evaluación ----------------------------
def generate_cypher(question: str, engine: str, model: str | None) -> tuple[str, bool]:
    """
    engine ∈ {'rules','rules_fb','gpt','gpt_fb'}
    Devuelve (cypher, fallback_used)
    """
    fb_used = False
    cy = None
    if engine.startswith("rules"):
        cy = rules_gen(question)
    elif engine.startswith("gpt"):
        cy = gpt_nl2cypher(question, model_hint=model)

    if (not cy) or cy == "FALLBACK":
        if engine.endswith("_fb"):
            fb = _fallback_from_question(question)
            if fb:
                cy = fb
                fb_used = True

    if not cy or cy == "FALLBACK":
        return "", fb_used

    # NORMALIZADORES
    cy = _fix_label_props_to_alias(_normalize_return(cy))
    if engine.startswith("gpt"):
        cy = _enforce_document_return(cy)  # fuerza id/titulo para GPT

    # Rescue extra SOLO para gpt_fb: si GPT trae pocas filas, probamos Reglas
    if engine == "gpt_fb":
        n_gpt = _quick_count(cy)
        if n_gpt <= 1:
            cy_rules = rules_gen(question)
            cy_rules = _fix_label_props_to_alias(_normalize_return(cy_rules))
            if is_safe_cypher(cy_rules):
                n_rules = _quick_count(cy_rules)
                if n_rules > n_gpt:
                    cy = cy_rules
                    fb_used = True  # indicamos que hubo fallback/rescate

    if not is_safe_cypher(cy):
        # por seguridad, no ejecutamos writes
        return "", fb_used
    return cy, fb_used

def run_eval_row(row, engines: list[str], model: str | None):
    qid = row.get("qid", None)
    question = str(row["question"]).strip()
    gt_type = str(row.get("gt_type", "")).strip().lower()
    gt_payload = str(row.get("gt_payload", "")).strip()

    # gold
    has_gold = (gt_type == "ids") and bool(gt_payload)
    gold_ids = set([s.strip() for s in gt_payload.split("|") if s.strip()]) if has_gold else set()
    gold_ids = set(map(str.lower, gold_ids))

    outputs = []
    for eng in engines:
        rec = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "qid": qid,
            "question": question,
            "engine": eng,
            "model": model if eng.startswith("gpt") else None,
            "fallback_used": False,
            "status": "error",
            "precision": None,
            "recall": None,
            "f1": None,
            "rows": 0,
            "ms": None,
            "cypher": "",
            "error": None,
        }
        cy, fb_used = generate_cypher(question, eng, model)
        rec["fallback_used"] = fb_used
        rec["cypher"] = cy
        if not cy:
            rec["error"] = "no_cypher"
            outputs.append(rec)
            continue

        t0 = time.time()
        try:
            df = run_cypher(cy).to_data_frame()
            ms = int((time.time() - t0) * 1000)
            rec["ms"] = ms
            rec["rows"] = 0 if df is None else len(df)
            pred_ids = extract_ids(df)
            pred_ids = set(map(str.lower, pred_ids))
            if has_gold:
                p, r, f1 = prf(pred_ids, gold_ids)
                rec["precision"], rec["recall"], rec["f1"] = p, r, f1
            rec["status"] = "ok"
        except Exception as e:
            rec["error"] = str(e)
        outputs.append(rec)
    return outputs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=str, default=str(DATA_CSV), help="CSV de preguntas (qid,question,gt_type,gt_payload,notes)")
    ap.add_argument("--engines", nargs="+", default=["rules","rules_fb","gpt","gpt_fb"],
                    help="Motores a evaluar (rules, rules_fb, gpt, gpt_fb)")
    ap.add_argument("--model", type=str, default=(st.secrets.get("OPENAI_MODEL") or "gpt-4o-mini"),
                    help="Modelo GPT si se usan motores gpt/gpt_fb")
    ap.add_argument("--limit", type=int, default=0, help="Evalúa solo N preguntas (>0); 0 = todas")
    args = ap.parse_args()

    # Warm-up graph (asegura esquema)
    get_graph()

    dfq = pd.read_csv(args.data).fillna("")
    if args.limit and args.limit > 0:
        dfq = dfq.head(args.limit)

    all_rows = []
    for _, row in dfq.iterrows():
        outs = run_eval_row(row, args.engines, args.model)
        all_rows.extend(outs)

    # Guarda JSONL
    with open(RESULTS_JSONL, "w", encoding="utf-8") as f:
        for r in all_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    dfr = pd.DataFrame(all_rows)
    # Resumen por motor
    if not dfr.empty:
        def _q95(s):
            try: return float(pd.Series(s).quantile(0.95))
            except: return None

        grp = dfr.groupby("engine", dropna=False).agg(
            n=("status","count"),
            ok=("status", lambda s: int((s=="ok").sum())),
            error=("status", lambda s: int((s=="error").sum())),
            fallback=("fallback_used", lambda s: int(s.sum())),
            f1_avg=("f1","mean"),
            f1_median=("f1","median"),
            ms_p50=("ms", lambda s: float(pd.Series(s).median(skipna=True))),
            ms_p95=("ms", _q95),
            rows_avg=("rows","mean"),
        ).reset_index()
        grp["ok_rate"] = grp["ok"] / grp["n"]
        grp.to_csv(SUMMARY_CSV, index=False)
        print("\n=== Resumen por motor ===")
        print(grp.to_string(index=False))
    else:
        print("No se generaron resultados.")

    print(f"\nGuardado: {RESULTS_JSONL}")
    print(f"Guardado: {SUMMARY_CSV}")


if __name__ == "__main__":
    main()
