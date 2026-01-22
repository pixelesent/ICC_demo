# =========================
# IMPORTS
# =========================
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

SHEETS = {
    "PRODUCTOS_TERMINADOS": "PRODUCTOS_TERMINADOS",
    "COMPONENTES_EMPAQUE": "COMPONENTES_EMPAQUE",
    "MATERIAS_PRIMAS": "MATERIAS_PRIMAS",
    "BOM_EMPAQUE": "BOM_EMPAQUE",
    "FORMULA_MP": "FORMULA_MP",
    "PEDIDOS_CLIENTES": "PEDIDOS_CLIENTES",
    "HISTORIAL_VENTAS": "HISTORIAL_VENTAS",
    "MEZCLADORAS": "MEZCLADORAS",
    "LLENADORAS": "LLENADORAS",
}

DEFAULT_TOLERANCE_DAYS = 3


# =========================
# DATA LOADING (CSV PUBLIC)
# =========================
def read_sheet(tab: str) -> pd.DataFrame:
    url = (
        f"https://docs.google.com/spreadsheets/d/"
        f"{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&sheet={tab}"
    )
    return pd.read_csv(url)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
    )
    return df


@st.cache_data(ttl=30)
def load_all():
    data = {}
    for k, tab in SHEETS.items():
        data[k] = normalize_columns(read_sheet(tab))
    return data


# =========================
# SAFE CASTS
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


def to_date(x):
    try:
        return dtparse(str(x)).date()
    except Exception:
        return None


# =========================
# CORE LOGIC
# =========================
def consolidate_weekly_demand(pedidos, start, end):
    pedidos["FECHA_INGRESO"] = pedidos["FECHA_INGRESO"].apply(to_date)
    pedidos["CANTIDAD_NUM"] = pedidos["CANTIDAD"].apply(to_int)

    mask = (pedidos["FECHA_INGRESO"] >= start) & (pedidos["FECHA_INGRESO"] <= end)

    return (
        pedidos.loc[mask]
        .groupby("SKU", as_index=False)["CANTIDAD_NUM"]
        .sum()
        .rename(columns={"CANTIDAD_NUM": "DEMANDA_BRUTA"})
    )


def net_demand(demanda, pt):
    pt["INVENTARIO_NUM"] = pt["INVENTARIO"].apply(to_int)

    df = demanda.merge(
        pt[["SKU", "INVENTARIO_NUM"]],
        on="SKU",
        how="left",
    )

    df["INVENTARIO_NUM"] = df["INVENTARIO_NUM"].fillna(0)
    df["DEMANDA_NETA"] = (df["DEMANDA_BRUTA"] - df["INVENTARIO_NUM"]).clip(lower=0)

    return df


def packaging_explosion(demanda, bom, comp, week_end, tolerance_days):
    bom["QTY"] = bom["CANTIDAD_POR_UNIDAD"].apply(to_float)

    comp["INV"] = comp["INVENTARIO"].apply(to_int)
    comp["WIP"] = comp["EN_PROCESO"].apply(to_int)
    comp["ETA"] = comp["FECHA_ESTIMADA"].apply(to_date)

    exp = demanda.merge(bom, on="SKU", how="left")
    exp["REQ"] = exp["DEMANDA_NETA"] * exp["QTY"]

    exp = exp.merge(
        comp[["COMPONENTE_ID", "INV", "WIP", "ETA"]],
        on="COMPONENTE_ID",
        how="left",
    )

    tolerance_date = week_end + pd.Timedelta(days=tolerance_days)

    def status(row):
        if row["REQ"] <= row["INV"]:
            return "OK"
        if row["ETA"] and row["ETA"] <= tolerance_date:
            if row["INV"] + row["WIP"] >= row["REQ"]:
                return "RIESGO"
        return "BLOQUEADO"

    exp["ESTADO"] = exp.apply(status, axis=1)

    resumen = []
    for sku, g in exp.groupby("SKU"):
        estados = set(g["ESTADO"])
        if "BLOQUEADO" in estados:
            final = "BLOQUEADO"
        elif "RIESGO" in estados:
            final = "RIESGO"
        else:
            final = "OK"
        resumen.append({"SKU": sku, "ESTADO_EMPAQUE": final})

    return pd.DataFrame(resumen)


def compute_cem(hist):
    hist["UNIDADES_NUM"] = hist["UNIDADES_VENDIDAS"].apply(to_float)

    return (
        hist.groupby("SKU", as_index=False)["UNIDADES_NUM"]
        .mean()
        .rename(columns={"UNIDADES_NUM": "CEM"})
    )


# =========================
# IA
# =========================
IA_SYSTEM_PROMPT = """
Eres un ingeniero senior de planificación en industria cosmética.

Reglas:
- Nunca producir si Estado_Empaque = BLOQUEADO
- Usa criterio humano
- Devuelve SOLO JSON

Formato:
{
  "decision": "PRODUCIR" | "PRODUCIR_CON_RIESGO" | "NO_PRODUCIR",
  "razon": "string corto",
  "confianza": 0.0-1.0
}
"""


def ia_decide(row):
    if row["ESTADO_EMPAQUE"] == "BLOQUEADO":
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
st.title("🧪 Demo Planificación Semanal Asistida – ICC")

with st.sidebar:
    start = st.date_input("Inicio semana", value=date.today())
    end = st.date_input("Fin semana", value=date.today())
    tolerance = st.number_input(
        "Tolerancia empaque (días)",
        0,
        14,
        DEFAULT_TOLERANCE_DAYS,
    )
    run = st.button("Ejecutar planificación", type="primary")

if run:
    data = load_all()

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
    out["DECISION_IA"] = decisions.apply(lambda x: x["decision"])
    out["RAZON_IA"] = decisions.apply(lambda x: x["razon"])
    out["CONFIANZA"] = decisions.apply(lambda x: x["confianza"])

    st.dataframe(
        out[
            [
                "SKU",
                "DEMANDA_NETA",
                "ESTADO_EMPAQUE",
                "DECISION_IA",
                "RAZON_IA",
                "CONFIANZA",
            ]
        ],
        use_container_width=True,
    )
else:
    st.info("Presiona **Ejecutar planificación** para correr el demo.")
