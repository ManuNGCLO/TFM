# app/streamlit_app.py
import streamlit as st

st.set_page_config(page_title="Grafo Jurídico – TFM R46", layout="wide")
st.title("Grafo Jurídico – TFM R46")

st.write("Selecciona una sección desde el menú de la izquierda o usa los accesos rápidos:")

# 🔗 Enlaces correctos: RUTA RELATIVA AL ENTRYPOINT (este archivo está en /app)
st.page_link("pages/0_🔎_Consulta.py",
             label="🧭 Consultar")
st.page_link("pages/1_📥_Ingesta_PDF_a_Artículos.py",
             label="📥 Ingesta PDF → Documento + Artículos")
st.page_link("pages/2_🩺_Diagnóstico_Grafo.py",
             label="🩺 Explorar / Diagnóstico")

st.page_link("pages/3_📈_Métricas_NL2Cypher.py", label="📊 Métricas")

st.page_link("pages/4_🕸️_Explorar_Grafo.py", label="🕸️ Grafo")
st.page_link("pages/5_🧠_Respuesta_Explicada.py", label="Explicada")
