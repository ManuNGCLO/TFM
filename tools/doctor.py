# tools/doctor.py
import os
import sys
import importlib
from textwrap import indent

OK = "✅"
FAIL = "❌"

def check_path():
    here = os.path.abspath(os.path.dirname(__file__))
    root = os.path.abspath(os.path.join(here, ".."))
    print(f"{OK} Proyecto en: {root}")
    return root

def check_files(root):
    expected = [
        ("streamlit_app.py", True),
        ("pages/0_🔎_Consulta.py", True),
        ("pages/1_📥_Ingesta_PDF_a_Artículos.py", True),
        ("pages/2_🩺_Diagnóstico_Grafo.py", True),
        ("pages/3_📈_Métricas_NL2Cypher.py", True),
        ("pages/4_🕸️_Explorar_Grafo.py", True),
        ("utils/graph_client.py", True),
        ("utils/text_to_cypher.py", True),
        ("requirements.txt", True),
        (".streamlit/config.toml", True),
        ("cypher/constraints.cypher", False),  # opcional pero recomendado
    ]
    ok = True
    for rel, required in expected:
        p = os.path.join(root, rel)
        if os.path.exists(p):
            print(f"{OK} {rel}")
        else:
            if required:
                print(f"{FAIL} Falta: {rel}")
                ok = False
            else:
                print(f"ℹ️  (Opcional) No está {rel}")
    return ok

def check_imports():
    mods = [
        "streamlit",
        "py2neo",
        "pandas",
        "numpy",
        "fitz",    # pymupdf
        "networkx",
        "plotly",
        "openai",
        "pyvis.network",
    ]
    ok = True
    for m in mods:
        try:
            importlib.import_module(m)
            print(f"{OK} import {m}")
        except Exception as e:
            ok = False
            print(f"{FAIL} import {m}: {e}")
    return ok

def check_secrets():
    # Streamlit los entrega via st.secrets. Aquí solo comprobamos env variables
    neo_ok = all(os.getenv(k) for k in ("NEO4J_URI", "NEO4J_USER", "NEO4J_PASS"))
    ai_ok  = os.getenv("OPENAI_API_KEY") is not None
    if neo_ok:
        print(f"{OK} Variables Neo4j presentes (env o secrets).")
    else:
        print(f"{FAIL} Falta NEO4J_URI/USER/PASS (en .streamlit secrets o env).")
    if ai_ok:
        print(f"{OK} OPENAI_API_KEY presente.")
    else:
        print("ℹ️  No hay OPENAI_API_KEY (Solo reglas locales funcionarán).")
    return neo_ok

def main():
    root = check_path()
    ok_files = check_files(root)
    ok_imports = check_imports()
    ok_secrets = check_secrets()

    print("\nResumen:")
    print(indent(f"Archivos: {'OK' if ok_files else 'ERROR'}", "  "))
    print(indent(f"Imports: {'OK' if ok_imports else 'ERROR'}", "  "))
    print(indent(f"Secrets: {'OK' if ok_secrets else 'FALTAN'}", "  "))

    if not (ok_files and ok_imports):
        sys.exit(1)

if __name__ == "__main__":
    main()
