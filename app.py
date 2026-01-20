
import json
from datetime import datetime, date
from dateutil.parser import parse as dtparse

import numpy as np
import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials

from openai import OpenAI


# =========================
# CONFIG
# =========================
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

# “Ventana tolerable” por defecto (puedes moverlo a la UI)
DEFAULT_TOLERANCE_DAYS = 3


# =========================
# GOOGLE SHEETS
# =========================
@st.cache_resource
def get_gspread_client():
    creds_info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def read_sheet_tab_as_df(gc, spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab_name)
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return pd.DataFrame()
    headers = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=headers)
    df = df.replace({"": np.nan})
    return df

@st.cache_data(ttl=10, show_spinner=False)
def load_all_tabs(spreadsheet_id: str) -> dict[str, pd.DataFrame]:
    gc = get_gspread_client()
    data = {}
    for k, tab in SHEETS_TABS.items():
        data[k] = read_sheet_tab_as_df(gc, spreadsheet_id, tab)
    return data


# =========================
# HELPERS (tipos / limpieza)
# =========================
def to_int(x, default=0):
    try:
        if pd.isna(x): return default
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return default

def to_float(x, default=0.0):
    try:
        if pd.isna(x): return default
        return float(str(x).replace(",", "").strip())
    except Exception:
        return default

def to_date_safe(x):
    if pd.isna(x): return None
    try:
        # admite "2026-01-20" o datetime
        d = dtparse(str(x)).date()
        return d
    except Exception:
        return None


# =========================
# CORE LOGIC (determinística + IA solo criterio)
# =========================
def consolidate_weekly_demand(pedidos: pd.DataFrame, week_start: date, week_end: date) -> pd.DataFrame:
    """
    Espera (ideal) columnas: SKU, Cantidad, Fecha_Requerida, Prioridad_Cliente, Cliente
    Puedes renombrar si en tu demo usas otros headers.
    """
    if pedidos.empty:
        return pd.DataFrame(columns=["SKU", "Demanda_Bruta"])

    df = pedidos.copy()

    # Normaliza nombres esperados (si vienen distintos, ajústalo aquí)
    # df = df.rename(columns={...})
    required_cols = ["SKU", "Cantidad", "Fecha_Requerida"]
    for c in required_cols:
        if c not in df.columns:
            # si falta, devolvemos vacío para que el demo no explote
            return pd.DataFrame(columns=["SKU", "Demanda_Bruta"])

    df["Fecha_Requerida_dt"] = df["Fecha_Requerida"].apply(to_date_safe)
    df["Cantidad_num"] = df["Cantidad"].apply(to_int)

    df = df[df["Fecha_Requerida_dt"].notna()]
    df = df[(df["Fecha_Requerida_dt"] >= week_start) & (df["Fecha_Requerida_dt"] <= week_end)]

    agg = df.groupby("SKU", as_index=False)["Cantidad_num"].sum()
    agg = agg.rename(columns={"Cantidad_num": "Demanda_Bruta"})
    return agg

def net_demand(demanda_bruta: pd.DataFrame, pt: pd.DataFrame) -> pd.DataFrame:
    """
    Espera PT: SKU, Inventario
    """
    if demanda_bruta.empty:
        return pd.DataFrame(columns=["SKU", "Demanda_Bruta", "Inventario_PT", "Demanda_Neta"])

    df = demanda_bruta.copy()
    if pt.empty or ("SKU" not in pt.columns) or ("Inventario" not in pt.columns):
        df["Inventario_PT"] = 0
        df["Demanda_Neta"] = df["Demanda_Bruta"].apply(lambda x: max(0, to_int(x)))
        return df

    pt2 = pt.copy()
    pt2["Inventario_num"] = pt2["Inventario"].apply(to_int)
    pt2 = pt2[["SKU", "Inventario_num"]].dropna(subset=["SKU"])
    df = df.merge(pt2, on="SKU", how="left")
    df["Inventario_num"] = df["Inventario_num"].fillna(0).astype(int)

    df["Inventario_PT"] = df["Inventario_num"]
    df["Demanda_Neta"] = (df["Demanda_Bruta"].apply(to_int) - df["Inventario_num"]).clip(lower=0).astype(int)
    df = df.drop(columns=["Inventario_num"])
    return df

def packaging_explosion(demanda_neta: pd.DataFrame, bom: pd.DataFrame, comp: pd.DataFrame, week_end: date, tolerance_days: int) -> pd.DataFrame:
    """
    BOM_EMPAQUE: SKU, Componente, Cantidad_por_unidad
    COMPONENTES_EMPAQUE: Componente, Inventario, En_Proceso, Fecha_Estimada

    Output por SKU:
      - Estado_Empaque: OK / RIESGO / BLOQUEADO
      - Detalle_Empaque (resumen)
    """
    if demanda_neta.empty:
        return pd.DataFrame(columns=["SKU", "Estado_Empaque", "Detalle_Empaque"])

    # Validaciones mínimas
    for c in ["SKU", "Componente", "Cantidad_por_unidad"]:
        if bom.empty or c not in bom.columns:
            out = demanda_neta[["SKU"]].copy()
            out["Estado_Empaque"] = "RIESGO"
            out["Detalle_Empaque"] = "BOM_EMPAQUE incompleta"
            return out

    for c in ["Componente", "Inventario", "En_Proceso", "Fecha_Estimada"]:
        if comp.empty or c not in comp.columns:
            out = demanda_neta[["SKU"]].copy()
            out["Estado_Empaque"] = "RIESGO"
            out["Detalle_Empaque"] = "COMPONENTES_EMPAQUE incompleta"
            return out

    bom2 = bom.copy()
    bom2["QtyPer"] = bom2["Cantidad_por_unidad"].apply(to_float)
    bom2 = bom2.dropna(subset=["SKU", "Componente"])

    comp2 = comp.copy()
    comp2["Inv"] = comp2["Inventario"].apply(to_int)
    comp2["WIP"] = comp2["En_Proceso"].apply(to_int)
    comp2["ETA"] = comp2["Fecha_Estimada"].apply(to_date_safe)
    comp2 = comp2[["Componente", "Inv", "WIP", "ETA"]].dropna(subset=["Componente"])

    dn = demanda_neta.copy()
    dn["Demanda_Neta_num"] = dn["Demanda_Neta"].apply(to_int)

    # Explosión: requerimiento por componente y SKU
    exp = dn.merge(bom2, on="SKU", how="left")
    exp["Req"] = (exp["Demanda_Neta_num"] * exp["QtyPer"]).fillna(0).astype(float)

    # Une inventarios de componentes
    exp = exp.merge(comp2, on="Componente", how="left")

    # Para cada componente, disponibilidad “esta semana”:
    # - disponible hoy = Inv
    # - disponible con WIP si ETA <= week_end + tolerance
    tolerance_date = week_end + pd.Timedelta(days=tolerance_days)

    def comp_status(row):
        req = float(row.get("Req", 0) or 0)
        inv = int(row.get("Inv", 0) or 0)
        wip = int(row.get("WIP", 0) or 0)
        eta = row.get("ETA", None)

        if req <= 0:
            return ("OK", "")
        if inv >= req:
            return ("OK", f"{row['Componente']}: OK (inv)")
        # falta hoy, vemos si llega “a tiempo” con WIP
        if eta is not None and eta <= tolerance_date.date():
            if (inv + wip) >= req:
                return ("RIESGO", f"{row['Componente']}: RIESGO (llega {eta.isoformat()})")
            else:
                return ("BLOQUEADO", f"{row['Componente']}: BLOQUEADO (insuficiente aun con WIP)")
        return ("BLOQUEADO", f"{row['Componente']}: BLOQUEADO (ETA fuera de ventana o desconocida)")

    exp[["CompEstado", "CompNota"]] = exp.apply(lambda r: pd.Series(comp_status(r)), axis=1)

    # Agrega a nivel SKU: regla dura
    # - si cualquier componente BLOQUEADO => SKU BLOQUEADO
    # - si no hay bloqueados pero hay RIESGO => SKU RIESGO
    # - si todo OK => OK
    sku_states = []
    for sku, g in exp.groupby("SKU"):
        estados = set(g["CompEstado"].dropna().tolist())
        notas = [n for n in g["CompNota"].dropna().tolist() if n]

        if "BLOQUEADO" in estados:
            s = "BLOQUEADO"
        elif "RIESGO" in estados:
            s = "RIESGO"
        else:
            s = "OK"

        sku_states.append({
            "SKU": sku,
            "Estado_Empaque": s,
            "Detalle_Empaque": " | ".join(notas[:6])  # limita texto
        })

    return pd.DataFrame(sku_states)

def compute_cem(hist: pd.DataFrame) -> pd.DataFrame:
    """
    Demo simple: CEM por SKU = promedio mensual de 24 meses.
    Espera: SKU, Mes, Ventas (o Unidades)
    """
    if hist.empty or "SKU" not in hist.columns:
        return pd.DataFrame(columns=["SKU", "CEM"])

    # Ajusta nombres si tu demo usa otros headers
    sales_col = None
    for c in ["Ventas", "Unidades", "Cantidad"]:
        if c in hist.columns:
            sales_col = c
            break
    if sales_col is None:
        return pd.DataFrame(columns=["SKU", "CEM"])

    df = hist.copy()
    df["Sales"] = df[sales_col].apply(to_float)
    cem = df.groupby("SKU", as_index=False)["Sales"].mean()
    cem = cem.rename(columns={"Sales": "CEM"})
    return cem

def deterministic_validation_placeholder(demanda: pd.DataFrame, mezcl: pd.DataFrame, llen: pd.DataFrame) -> pd.DataFrame:
    """
    Este paso SOLO valida compatibilidad/capacidad sin optimizar.
    Para el demo base lo dejamos como “OK” siempre, pero la columna queda lista.
    """
    out = demanda[["SKU"]].copy()
    out["Estado_Capacidad"] = "OK"
    return out


# =========================
# IA (solo decisión)
# =========================
IA_SYSTEM_PROMPT = """Eres un ingeniero senior de planificación de producción en industria cosmética.
Tu tarea es DECIDIR (no calcular) si producir un SKU esta semana.
Reglas inviolables:
- Nunca propongas producir si el estado de empaque es BLOQUEADO.
- No hagas cálculos detallados; usa los números como contexto.
- Devuelve SOLO JSON válido (sin markdown).
Salida requerida:
{
  "decision": "PRODUCIR" | "PRODUCIR_CON_RIESGO" | "NO_PRODUCIR",
  "razon": "string corta, humana",
  "confianza": 0.0-1.0
}
"""

def call_ia_decision(payload: dict) -> dict:
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    model = st.secrets.get("OPENAI_MODEL", "gpt-4.1-mini")

    resp = client.chat.completions.create(
        model=model,
        temperature=0.2,
        messages=[
            {"role": "system", "content": IA_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        response_format={"type": "json_object"},
    )
    txt = resp.choices[0].message.content
    try:
        return json.loads(txt)
    except Exception:
        return {"decision": "NO_PRODUCIR", "razon": "Respuesta IA inválida (fallback).", "confianza": 0.0}

def decide_for_row(row: pd.Series) -> dict:
    # Regla dura previa (antes de llamar IA)
    if row.get("Estado_Empaque") == "BLOQUEADO":
        return {"decision": "NO_PRODUCIR", "razon": "Empaque BLOQUEADO (restricción inviolable).", "confianza": 1.0}

    payload = {
        "semana": row.get("Semana"),
        "sku": row.get("SKU"),
        "demanda_neta": int(row.get("Demanda_Neta") or 0),
        "estado_empaque": row.get("Estado_Empaque"),
        "detalle_empaque": row.get("Detalle_Empaque"),
        "cem": float(row.get("CEM") or 0.0),
        "prioridad_cliente": row.get("Prioridad_Cliente", None),
        "fecha_requerida_min": row.get("Fecha_Requerida_Min", None),
        "riesgo_operativo": row.get("Riesgo_Operativo", "medio"),
        "notas": "Decide con criterio humano. Si hay RIESGO, puedes permitir producir con riesgo si la demanda/presión lo amerita.",
    }
    return call_ia_decision(payload)


# =========================
# STREAMLIT UI
# =========================
st.set_page_config(page_title="Demo Planificación Semanal (IA Asistida)", layout="wide")

st.title("🧪 Demo – Planificación Semanal Asistida por IA (Cosmética)")
st.caption("Empaque manda. La IA decide solo en zonas grises. Google Sheets = base viva.")

with st.sidebar:
    st.header("⚙️ Parámetros")
    spreadsheet_id = st.secrets["GSPREAD_SHEET_ID"]
    tolerance_days = st.number_input("Tolerancia llegada empaque (días)", min_value=0, max_value=14, value=DEFAULT_TOLERANCE_DAYS)
    week_start = st.date_input("Inicio de semana", value=date.today())
    week_end = st.date_input("Fin de semana", value=date.today())
    run_btn = st.button("🚀 Ejecutar planificación semanal", type="primary")

st.divider()

if run_btn:
    with st.spinner("Leyendo Google Sheets y ejecutando lógica…"):
        data = load_all_tabs(spreadsheet_id)

        pedidos = data["PEDIDOS_CLIENTES"]
        pt = data["PRODUCTOS_TERMINADOS"]
        bom = data["BOM_EMPAQUE"]
        comp = data["COMPONENTES_EMPAQUE"]
        hist = data["HISTORIAL_VENTAS"]
        mezcl = data["MEZCLADORAS"]
        llen = data["LLENADORAS"]

        # 1) Demanda bruta
        demanda_bruta = consolidate_weekly_demand(pedidos, week_start, week_end)

        # 2) Demanda neta
        demanda_neta_df = net_demand(demanda_bruta, pt)

        # 3.1) Explosión empaque
        empaque_status = packaging_explosion(demanda_neta_df, bom, comp, week_end, tolerance_days)

        # CEM
        cem = compute_cem(hist)

        # Merge final
        out = demanda_neta_df.merge(empaque_status, on="SKU", how="left")
        out = out.merge(cem, on="SKU", how="left")
        out["CEM"] = out["CEM"].fillna(0.0)

        # Placeholder capacidad (6)
        cap = deterministic_validation_placeholder(out, mezcl, llen)
        out = out.merge(cap, on="SKU", how="left")

        # Campos demo extra (si existen en pedidos, puedes enriquecer)
        out["Semana"] = f"{week_start.isoformat()} → {week_end.isoformat()}"

        # 7) IA decide (solo si no está bloqueado; bloqueado ya viene NO_PRODUCIR)
        decisions = []
        for _, r in out.iterrows():
            d = decide_for_row(r)
            decisions.append(d)

        out["Decisión_IA"] = [d.get("decision") for d in decisions]
        out["Razón_IA"] = [d.get("razon") for d in decisions]
        out["Confianza"] = [d.get("confianza") for d in decisions]

        # Orden/selección columnas output
        show_cols = [
            "Semana", "SKU", "Demanda_Neta", "Estado_Empaque",
            "Decisión_IA", "Razón_IA", "Confianza"
        ]
        for c in show_cols:
            if c not in out.columns:
                out[c] = None

    st.success("Listo. Cambia inventarios/fechas/pedidos en Sheets y vuelve a ejecutar.")
    st.dataframe(out[show_cols], use_container_width=True, hide_index=True)

    with st.expander("🔎 Debug (muestras de inputs)"):
        st.write("DEMANDA_BRUTA_SEMANAL")
        st.dataframe(demanda_bruta, use_container_width=True, hide_index=True)
        st.write("DEMANDA_NETA")
        st.dataframe(demanda_neta_df, use_container_width=True, hide_index=True)
        st.write("Estado empaque por SKU")
        st.dataframe(empaque_status, use_container_width=True, hide_index=True)

else:
    st.info("Edita datos en Google Sheets y luego presiona **Ejecutar planificación semanal**.")
