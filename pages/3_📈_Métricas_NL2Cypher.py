# -*- coding: utf-8 -*-
# app/pages/3_📈_Métricas_NL2Cypher.py
from __future__ import annotations

import json, os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

LOG_PATH = os.path.join("data", "history_nl2cypher.jsonl")

st.set_page_config(page_title="Métricas NL→Cypher", layout="wide")

# --- Cabecera compacta (título sin recortes) ---
st.markdown("""
<div class="app-header">
  <span class="app-icon">📈</span>
  <h1 class="app-title">Métricas NL → Cypher</h1>
</div>
""", unsafe_allow_html=True)

# --- Estilos ---
st.markdown("""
<style>
.app-header{
  display:flex; align-items:center; gap:.6rem;
  margin:.25rem 0 1.1rem 0;
}
.app-icon{
  font-size:1.5rem; line-height:1; transform: translateY(2px);
}
.app-title{
  margin:.15rem 0 0 0;
  font-weight:800;
  letter-spacing:-0.01em;
  font-size:clamp(20px, 3.2vw, 30px);
  line-height:1.30;
  white-space:normal;
  overflow-wrap:anywhere;
}
.block-container{ padding-top:2.2rem; }
.dataframe tbody tr th{ display:none; }
.app-sep{ border:0; border-top:1px solid rgba(255,255,255,.08); margin: 1.1rem 0; }
</style>
""", unsafe_allow_html=True)

# ---------- Carga segura del histórico ----------
rows = []
if os.path.exists(LOG_PATH):
    with open(LOG_PATH, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass

if not rows:
    st.info("Aún no hay histórico online (history_nl2cypher.jsonl). Ejecuta consultas en la página **Consulta** para generar telemetría.")
    df = pd.DataFrame()
else:
    df = pd.DataFrame(rows)

# Tipos y saneo básico
if not df.empty:
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    for col in ("rows", "ms"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

# ---------- Filtros superiores (solo si hay histórico) ----------
if not df.empty:
    colA, colB, colC, colD = st.columns([1, 1, 1, 2])
    with colA:
        days = st.selectbox("Rango de días", [1, 3, 7, 14, 30, 90], index=2)
        tmin = datetime.now() - timedelta(days=days)
    with colB:
        engine = st.selectbox("Motor", ["Todos", "Reglas locales", "GPT (OpenAI)"], index=0)
    with colC:
        model = st.selectbox(
            "Modelo (si GPT)",
            ["Todos", "gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1", "o4-mini", "o3-mini"],
            index=0,
        )
    with colD:
        evtype = st.selectbox("Tipo de evento", ["Todos", "generate", "execute"], index=0)

    # Filtro por fecha y selecciones
    f = df.copy()
    f = f[(f["ts"].isna()) | (f["ts"] >= tmin)]
    if engine != "Todos":
        f = f[f["engine"] == engine]
    if model != "Todos":
        f = f[f["model"] == model]
    if evtype != "Todos":
        f = f[f["type"] == evtype]

    # Normalización para groupby/pivots
    f["engine"] = f["engine"].fillna("(desconocido)")
    f["model"]  = f["model"].fillna("(reglas)")

    # ---------- KPIs básicos ----------
    c1, c2, c3, c4 = st.columns(4)
    total   = len(f)
    ok      = int((f["status"] == "ok").sum())
    fallback= int((f["status"] == "fallback").sum())
    err     = int((f["status"] == "error").sum())

    with c1: st.metric("Eventos", total)
    with c2: st.metric("OK", ok)
    with c3: st.metric("Fallback", fallback)
    with c4: st.metric("Errores", err)

    st.markdown("<hr class='app-sep'/>", unsafe_allow_html=True)

    # ---------- Éxito por motor ----------
    st.subheader("Éxito por motor")
    if f.empty:
        st.info("No hay eventos en el rango/filtrado seleccionado.")
    else:
        grp = f.groupby(["engine", "type", "status"]).size().reset_index(name="n")
        pivot = grp.pivot_table(index=["engine", "type"], columns="status", values="n", fill_value=0).reset_index()
        for col in ("ok", "error", "fallback"):
            if col not in pivot.columns:
                pivot[col] = 0
        st.dataframe(pivot[["engine", "type", "error", "ok", "fallback"]],
                     use_container_width=True, height=220)

    # ---------- Latencia y filas (solo ejecuciones OK) ----------
    st.subheader("Latencia y filas (solo ejecuciones OK)")
    fx = f[(f["type"] == "execute") & (f["status"] == "ok")].copy()
    if fx.empty:
        st.info("No hay ejecuciones OK en el rango/filtrado activo.")
    else:
        agg = fx.groupby(["engine", "model"]).agg(
            n=("ms", "count"),
            ms_avg=("ms", "mean"),
            ms_p95=("ms", lambda s: s.quantile(0.95)),
            rows_avg=("rows", "mean"),
        ).reset_index()
        agg["ms_avg"]   = agg["ms_avg"].round(1)
        agg["ms_p95"]   = agg["ms_p95"].round(1)
        agg["rows_avg"] = agg["rows_avg"].round(1)
        st.dataframe(agg, use_container_width=True, height=260)

    # ---------- Top problemas ----------
    st.subheader("Top problemas")
    col1, col2 = st.columns(2)

    with col1:
        tb_fallback = (
            f[f["status"] == "fallback"]
            .groupby(["question", "engine", "model"])
            .size()
            .reset_index(name="veces")
            .sort_values("veces", ascending=False)
            .head(10)
        )
        st.caption("Preguntas con más fallbacks")
        if tb_fallback.empty:
            st.info("No hay fallbacks en el filtro actual.")
        else:
            st.dataframe(tb_fallback, use_container_width=True, height=260)

    with col2:
        tb_error = (
            f[f["status"] == "error"]
            .groupby(["error", "engine", "model"])
            .size()
            .reset_index(name="veces")
            .sort_values("veces", ascending=False)
            .head(10)
        )
        st.caption("Causas de error más comunes")
        if tb_error.empty:
            st.info("No hay errores en el filtro actual.")
        else:
            st.dataframe(tb_error, use_container_width=True, height=260)

    st.markdown("<hr class='app-sep'/>", unsafe_allow_html=True)

    # ---------- Últimos eventos ----------
    st.subheader("Últimos eventos")
    st.dataframe(
        f.sort_values("ts", ascending=False).head(50),
        use_container_width=True,
        height=420,
    )

    # ---------- Export ----------
    csv = f.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Descargar CSV filtrado",
                       data=csv,
                       file_name="nl2cypher_metrics.csv",
                       mime="text/csv")

else:
    st.info("Cuando exista histórico, verás aquí KPIs y tablas online.")
    st.markdown("<hr class='app-sep'/>", unsafe_allow_html=True)

# === Resultados offline de evaluación (results.jsonl) ========================
st.subheader("📦 Resultados de evaluación offline (questions.csv → results.jsonl)")

results_path = Path("results") / "results.jsonl"
summary_path = Path("results") / "summary.csv"

if results_path.exists():
    rows_off = []
    with open(results_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rows_off.append(json.loads(line))
            except Exception:
                pass
    if rows_off:
        dfr = pd.DataFrame(rows_off)
        c1, c2 = st.columns([2,1])
        with c1:
            st.write("### Tabla cruda (primeros 200)")
            st.dataframe(dfr.head(200), use_container_width=True, hide_index=True)
        with c2:
            st.write("### Conteos")
            st.write(dfr["engine"].value_counts())
            st.write(dfr["status"].value_counts())

        st.write("### Resumen por motor")
        if summary_path.exists():
            dfs = pd.read_csv(summary_path)
        else:
            # cálculo rápido si no existe summary.csv
            def _q95(s):
                try: return float(pd.Series(s).quantile(0.95))
                except: return None
            dfs = dfr.groupby("engine", dropna=False).agg(
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
            dfs["ok_rate"] = dfs["ok"] / dfs["n"]
        st.dataframe(dfs, use_container_width=True, hide_index=True)
    else:
        st.info("results.jsonl existe pero está vacío.")
else:
    st.info("No se encontró results/results.jsonl. Ejecuta: `python tools/eval_questions.py`")
