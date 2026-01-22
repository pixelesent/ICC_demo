import json
from datetime import date
from dateutil.parser import parse as dtparse

import pandas as pd
import numpy as np
import streamlit as st
from openai import OpenAI


# =========================
# CONFIG
# =========================
SPREADSHEET_ID = st.secrets["GSPREAD_SHEET_ID"]

SHEETS_TABS = {
    "PRODUCTOS_TERMINADOS": "PRODUCTOS_TERMINADOS",
    "COMPONENTES_EMPAQUE": "COMPONENTES_EMPAQUE",
    "MATERIAS_PRIMAS": "MATERIAS_PRIMAS",
    "PEDIDOS_CLIENTES": "PEDIDOS_CLIENTES",
    "HISTORIAL_VENTAS": "HISTORIAL_VENTAS",
    "BOM_EMPAQUE": "BOM_EMPAQUE",
    "FORMULA_MP": "FORMULA_MP",
    "MEZCLADORAS": "MEZCLADORAS",
    "LLENADORAS": "LLENADORAS",
}

DEFAULT_TOLERANCE_DAYS = 3


# =========================
# GOOGLE SHEETS (CSV PUBLIC)
# =========================
def read_sheet_csv(spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    url = (
        f"https://docs.google.com/spreadsheets/d/"
        f"{spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={tab_name}"
    )
    return pd.read_csv(url)


@st.cache_data(ttl=10)
def load_all_tabs(spreadsheet_id: str) -> dict:
    return {k: read_sheet_csv(spreadsheet_id, v) for k, v in SHEETS_TABS.items()}


# =========================
# HELPERS
# =========================
def to_int(x, default=0):
    try:
        return int(float(str(x).replace(",", "")))
    except Exception:
        return default


def to_float(x, default=0.0):
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return default


def to_date_safe(x):
    try:
        return dtparse(str(x)).date()
    except Exception:
        return None


# =========================
# CORE LOGIC
# =========================
def consolidate_weekly_demand(pedidos, start, end):
    pedidos["Fecha_dt"] = pedidos["Fecha_Requerida"].apply(to_date_safe)
    pedidos["Cantidad_num"] = pedidos["Cantidad"].apply(to_int)

    df = pedidos[
        (pedidos["Fecha_dt"] >= start) &
        (pedidos["Fecha_dt"] <= end)
    ]

    return (
        df.groupby("SKU", as_index=False)["Cantidad_num"]
        .sum()
        .rename(columns={"Cantidad_num": "Demanda_Bruta"})
    )


def net_demand(demanda, pt):
    pt["Inventario_num"] = pt["Inventario"].apply(to_int)

    df = demanda.merge(pt[["SKU", "Inventario_num"]], on="SKU", how="left")
    df["Inventario_num"] = df["Inventario_num"].fillna(0)
    df["Demanda_Neta"] = (
        df["Demanda_Bruta"] - df["Inventario_num"]
    ).clip(lower=0)

    return df


def packaging_explosion(demanda, bom, comp, week_end, tolerance_days):
    bom["Qty"] = bom["Cantidad_por_unidad"].apply(to_float)
    comp["Inv"] = comp["Inventario"].apply(to_int)
    comp["WIP"] = comp["En_Proceso"].apply(to_int)
    comp["ETA"] = comp["Fecha_Estimada"].apply(to_date_safe)

    exp = demanda.merge(bom, on="SKU", how="left")
    exp["Req"] = exp["Demanda_Neta"] * exp["Qty"]
    exp = exp.merge(comp, on="Componente", how="left")

    tolerance_date = week_end + pd.Timedelta(days=tolerance_days)

    def status(row):
        if row["Req"] <= row["Inv"]:
            return "OK"
        if row["ETA"] and row["ETA"] <= tolerance_date.date():
            if row["Inv"] + row["WIP"] >= row["Req"]:
                return "RIESGO"
        return "BLOQUEADO"

    exp["Estado"] = exp.apply(status, axis=1)

    sku_status = []
    for sku, g in exp.groupby("SKU"):
        estados = set(g["Estado"])
        if "BLOQUEADO" in estados:
            s = "BLOQUEADO"
        elif "RIESGO" in estados:
            s = "RIESGO"
        else:
            s = "OK"
        sku_status.append({"SKU": sku, "Estado_Empaque": s})

    return pd.DataFrame(sku_status)


def compute_cem(hist):
    hist["Ventas_num"] = hist["Ventas"].apply(to_float)
    return (
        hist.groupby("SKU", as_index=False)["Ventas_num"]
        .mean()
        .rename(columns={"Ventas_num": "CEM"})
    )


# =========================
# IA
# =========================
IA_SYSTEM_PROMPT = """
Eres un ingeniero senior de planificación en industria cosmética.
Decide si producir un SKU esta semana.

Reglas:
- Nunca producir si Estado_Empaque = BLOQUEADO
- No calcules, decide con criterio humano
- Devuelve SOLO JSON

Formato:
{
  "decision": "PRODUCIR" | "PRODUCIR_CON_RIESGO" | "NO_PRODUCIR",
  "razon": "string corto",
  "confianza": 0.0-1.0
}
"""


def ia_decide(row):
    if row["Estado_Empaque"] == "BLOQUEADO":
        return {
            "decision": "NO_PRODUCIR",
            "razon": "Empaque bloqueado",
            "confianza": 1.0,
        }

    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    resp = client.chat.completions.create(
        model=st.secrets.get("OPENAI_MODEL", "gpt-4.1-mini"),
        temperature=0.2,
        messages=[
            {"role": "system", "content": IA_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(row.to_dict())},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


# =========================
# STREAMLIT UI
# =========================
st.set_page_config(page_title="Demo Planificación IA", layout="wide")
st.title("🧪 Planificación Semanal Asistida por IA")

with st.sidebar:
    start = st.date_input("Inicio semana", value=date.today())
    end = st.date_input("Fin semana", value=date.today())
    tolerance = st.number_input("Tolerancia empaque (días)", 0, 14, DEFAULT_TOLERANCE_DAYS)
    run = st.button("Ejecutar planificación", type="primary")

if run:
    data = load_all_tabs(SPREADSHEET_ID)

    demanda_bruta = consolidate_weekly_demand(
        data["PEDIDOS_CLIENTES"], start, end
    )
    demanda_neta = net_demand(
        demanda_bruta, data["PRODUCTOS_TERMINADOS"]
    )
    empaque = packaging_explosion(
        demanda_neta,
        data["BOM_EMPAQUE"],
        data["COMPONENTES_EMPAQUE"],
        end,
        tolerance,
    )
    cem = compute_cem(data["HISTORIAL_VENTAS"])

    out = (
        demanda_neta
        .merge(empaque, on="SKU")
        .merge(cem, on="SKU", how="left")
    )

    decisions = out.apply(ia_decide, axis=1)
    out["Decisión_IA"] = decisions.apply(lambda x: x["decision"])
    out["Razón_IA"] = decisions.apply(lambda x: x["razon"])
    out["Confianza"] = decisions.apply(lambda x: x["confianza"])

    st.dataframe(
        out[
            [
                "SKU",
                "Demanda_Neta",
                "Estado_Empaque",
                "Decisión_IA",
                "Razón_IA",
                "Confianza",
            ]
        ],
        use_container_width=True,
    )
else:
    st.info("Presiona **Ejecutar planificación** para correr el demo.")
