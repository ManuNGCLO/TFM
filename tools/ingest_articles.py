# tools/ingest_articles.py
from pathlib import Path
import re, os
try:
    import tomllib as toml  # Py 3.11+
except ModuleNotFoundError:
    import tomli as toml    # Py <=3.10

import fitz
from py2neo import Graph

ROOT = Path(__file__).resolve().parents[1]
SECRETS_PATH = ROOT / ".streamlit" / "secrets.toml"
PDF_DIR = ROOT / "data" / "pdfs"

HINT_OVERRIDES = {
    "boe-a-2018-16673": "lo-3-2018",
    "boe-a-1999-23750-consolidado": "lo-15-1999",
    "celex_32016r0679_es_txt": "reglamento-ue-2016-679",
    "memoria-aepd-2024": "memoria-2024",
    "13500": "13500",
}

SOFT_HYPHEN = "\u00ad"; NBSP = "\u00a0"
ARTICLE_RE = re.compile(
    r'(?mi)^\s*(art(?:[íi]culo|\.)\s*(\d+(?:\s*(?:bis|ter|quater|quinquies|sexies))?))'
    r'(?:\s*[-–—.:]\s*(.*)|\s*$)'
)

def normalize_text(s: str) -> str:
    s = s.replace(SOFT_HYPHEN, "").replace(NBSP, " ")
    s = re.sub(r'(\w)-\n(\w)', r'\1\2', s)
    s = re.sub(r'(?mi)(art(?:[íi]culo|\.)\s*)\n\s*(\d+[a-z]?)', r'\1 \2', s)
    s = re.sub(r"[ \t]+", " ", s)
    return s

def load_pdf_text(pdf_path: Path, mode: str = "text") -> str:
    doc = fitz.open(pdf_path.as_posix())
    pages = []
    for p in doc:
        if mode == "blocks":
            blocks = p.get_text("blocks")
            blocks.sort(key=lambda b: (b[1], b[0]))
            page_text = "\n".join(b[4] for b in blocks if b[4].strip())
        else:
            page_text = p.get_text("text")
        pages.append(page_text)
    return normalize_text("\n".join(pages))

def split_articles(full_text: str):
    lines = full_text.splitlines()
    anchors = []
    for i, line in enumerate(lines):
        m = ARTICLE_RE.match(line)
        if m:
            anchors.append((i, m.group(2).strip(), (m.group(3) or "").strip() or "(sin título)"))
    arts = []
    for idx, (start_i, numero, titulo) in enumerate(anchors):
        end_i = anchors[idx+1][0] if idx+1 < len(anchors) else len(lines)
        cuerpo = "\n".join(lines[start_i+1:end_i]).strip()
        arts.append({"numero": numero, "titulo": titulo, "texto": cuerpo})
    return arts

def stem_to_hint(stem: str) -> str:
    return HINT_OVERRIDES.get(stem.lower(), stem.lower())

def load_graph() -> Graph:
    if SECRETS_PATH.exists():
        with open(SECRETS_PATH, "rb") as f:
            s = toml.load(f)
        uri = s.get("NEO4J_URI"); user = s.get("NEO4J_USER"); pwd = s.get("NEO4J_PASS")
    else:
        uri = os.getenv("NEO4J_URI"); user = os.getenv("NEO4J_USER","neo4j"); pwd = os.getenv("NEO4J_PASS")
    if not uri or not user or not pwd:
        raise RuntimeError("Faltan credenciales de Neo4j.")
    return Graph(uri, auth=(user, pwd))

def find_document_node(graph: Graph, hint: str):
    return graph.evaluate("""
        MATCH (d:Documento)
        WHERE toLower(d.id) CONTAINS $h
           OR toLower(coalesce(d.norm, d.titulo)) CONTAINS replace($h,'-',' ')
        RETURN d LIMIT 1
    """, h=hint.lower())

def upsert_articles_for_document(graph: Graph, doc_hint: str, pdf_path: Path):
    d = find_document_node(graph, doc_hint)
    if not d:
        print(f"[WARN] No encontré :Documento para hint='{doc_hint}' (file={pdf_path.name})")
        return
    mode = "blocks" if "reglamento-ue-2016-679" in doc_hint or "celex_32016r0679" in pdf_path.stem.lower() else "text"
    text = load_pdf_text(pdf_path, mode=mode)
    arts = split_articles(text)
    if not arts:
        print(f"[INFO] 0 artículos detectados en {pdf_path.name}.")
        return
    print(f"[OK]   {pdf_path.name}: {len(arts)} artículos → {doc_hint}")

    tx = graph.begin()
    for art in arts:
        exists = tx.evaluate("""
            MATCH (d:Documento)-[:TIENE_ARTICULO]->(a:Articulo {numero:$n})
            WHERE id(d)=$doc_id RETURN a
        """, n=art["numero"], doc_id=d.identity)
        if exists:
            tx.run("""
                MATCH (d:Documento)-[:TIENE_ARTICULO]->(a:Articulo {numero:$n})
                WHERE id(d)=$doc_id
                SET a.titulo=$t, a.texto=$x
            """, n=art["numero"], t=art["titulo"], x=art["texto"], doc_id=d.identity)
        else:
            tx.run("""
                MATCH (d:Documento) WHERE id(d)=$doc_id
                MERGE (d)-[:TIENE_ARTICULO]->(a:Articulo {numero:$n})
                ON CREATE SET a.titulo=$t, a.texto=$x
            """, doc_id=d.identity, n=art["numero"], t=art["titulo"], x=art["texto"])
    graph.commit(tx)
    print(f"[DONE] {pdf_path.name}: carga/actualización completada.")

if __name__ == "__main__":
    graph = load_graph()
    if not PDF_DIR.exists():
        print(f"[ERROR] No existe el directorio de PDFs: {PDF_DIR}"); raise SystemExit(1)
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"[ERROR] No se encontraron PDFs en {PDF_DIR}"); raise SystemExit(1)
    print(f"[INFO] Procesando {len(pdfs)} PDFs de {PDF_DIR} ...")
    for pdf in pdfs:
        hint = stem_to_hint(pdf.stem)
        upsert_articles_for_document(graph, hint, pdf)
