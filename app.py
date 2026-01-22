# ===========================
# app.py — DEMO FUNCIONAL
# Planificación Semanal Asistida por IA (Industria Cosmética)
# ===========================

import json
from datetime import date, timedelta
from dateutil.parser import parse as dtparse

import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

# ---------------- CONFIG ----------------
REQUIRED_SHEETS = [
    "PRODUCTOS_TERMINADOS",
    "COMPONENTES_EMPAQUE",
    "MATERIAS_PRIMAS",
    "PEDIDOS_CLIENTES",
    "HISTORIAL_VENTAS",
    "BOM_EMPAQUE",
    "FORMULA_MP",
    "MEZCLADORAS",
    "LLENADORAS",
]
TOLERANCE_DAYS = 2

# ---------------- HELPERS ----------------
def to_int(x, d=0):
    try:
        return int(float(x))
    except:
        return d

def to_float(x, d=0.0):
    try:
        return float(x)
    except:
        return d

def to_date(x):
    try:
        return dtparse(str(x)).date()
    except:
        return None

def week_bounds(d):
    start = d - timedelta(days=d.weekday())
    end = start + timedelta(days=6)
    return start, end

# ---------------- GSHEETS ----------------
def gs_client():
    secret = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]

    # Streamlit puede entregar dict, string, o cosa rara
    if isinstance(secret, dict):
        sa = secret
    else:
        # fuerza a string limpio
        sa = json.loads(str(secret))

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    return gspread.authorize(creds)

def load_sheets():
    sh = gs_client().open_by_key(st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"])
    titles = [w.title for w in sh.worksheets()]
    for r in REQUIRED_SHEETS:
        if r not in titles:
            raise RuntimeError(f"Falta pestaña {r}")
    out = {}
    for r in REQUIRED_SHEETS:
        out[r] = pd.DataFrame(sh.worksheet(r).get_all_records())
    return out

# ---------------- CORE ----------------
def consolidate_orders(ped, ws, we):
    df = ped.copy()
    df["FECHA_INGRESO"] = df["FECHA_INGRESO"].apply(to_date)
    df = df[(df["FECHA_INGRESO"] >= ws) & (df["FECHA_INGRESO"] <= we)]
    if df.empty:
        return pd.DataFrame(columns=["SKU", "Demanda_Bruta"])
    df["CANTIDAD"] = df["CANTIDAD"].apply(to_int)
    return df.groupby("SKU", as_index=False).agg(
        Demanda_Bruta=("CANTIDAD", "sum")
    )

def net_demand(dem, pt):
    base = dem.merge(
        pt[["SKU", "Inventario"]],
        on="SKU", how="left"
    )
    base["Inventario"] = base["Inventario"].fillna(0).apply(to_int)
    base["Demanda_Neta"] = (base["Demanda_Bruta"] - base["Inventario"]).clip(lower=0)
    return base

def explode_packaging(dn, bom, ce, we):
    if dn.empty:
        return pd.DataFrame(columns=["SKU", "Estado_Empaque", "Detalle_Empaque"])

    m = bom.merge(dn[["SKU", "Demanda_Neta"]], on="SKU")
    m["Req"] = m["CANTIDAD_POR_UNIDAD"].apply(to_float) * m["Demanda_Neta"]
    req = m.groupby("COMPONENTE_ID", as_index=False)["Req"].sum()

    ce2 = ce.copy()
    ce2["Inventario"] = ce2["Inventario"].apply(to_int)
    ce2["En_Proceso"] = ce2["En_Proceso"].apply(to_int)
    ce2["Fecha_Estimada"] = ce2["Fecha_Estimada"].apply(to_date)

    comp = req.merge(
        ce2[["Componente_ID", "Inventario", "En_Proceso", "Fecha_Estimada"]],
        on="Componente_ID", how="left"
    ).fillna({"Inventario": 0, "En_Proceso": 0})

    tol_end = we + timedelta(days=TOLERANCE_DAYS)

    def status(r):
        if r["Inventario"] >= r["Req"]:
            return "OK"
        if (r["Inventario"] + r["En_Proceso"]) >= r["Req"]:
            if r["Fecha_Estimada"] and r["Fecha_Estimada"] <= we:
                return "OK"
            if r["Fecha_Estimada"] and r["Fecha_Estimada"] <= tol_end:
                return "RIESGO"
        return "BLOQUEADO"

    comp["Estado"] = comp.apply(status, axis=1)

    sku_comp = m[["SKU", "COMPONENTE_ID"]].drop_duplicates().merge(
        comp[["Componente_ID", "Estado"]],
        on="Componente_ID", how="left"
    )

    sev = {"OK": 0, "RIESGO": 1, "BLOQUEADO": 2}
    sku_comp["sev"] = sku_comp["Estado"].map(sev)
    out = sku_comp.groupby("SKU", as_index=False)["sev"].max()
    out["Estado_Empaque"] = out["sev"].map({0: "OK", 1: "RIESGO", 2: "BLOQUEADO"})

    det = sku_comp.sort_values("sev", ascending=False).groupby("SKU").head(2)
    det = det.groupby("SKU")["COMPONENTE_ID"].apply(lambda s: ", ".join(s)).reset_index(name="Detalle_Empaque")

    return out.merge(det, on="SKU", how="left")

def calc_cem(hv):
    df = hv.copy()
    df["Fecha"] = df["Fecha"].apply(to_date)
    df["Unidades_vendidas"] = df["Unidades_vendidas"].apply(to_int)
    df = df[df["Fecha"].notna()]
    if df.empty:
        return pd.DataFrame(columns=["SKU", "CEM"])
    df["YM"] = df["Fecha"].apply(lambda d: f"{d.year}-{d.month:02d}")
    m = df.groupby(["SKU", "YM"], as_index=False)["Unidades_vendidas"].sum()
    return m.groupby("SKU", as_index=False)["Unidades_vendidas"].mean().rename(
        columns={"Unidades_vendidas": "CEM"}
    )

# ---------------- IA ----------------
def ai_decide(rows):
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    payload = rows.to_dict(orient="records")
    system = st.session_state["AI_SYSTEM"]

    resp = client.responses.create(
        model="gpt-4.1-mini",
        instructions=system,
        input=json.dumps({"items": payload}, ensure_ascii=False)
    )

    data = json.loads(resp.output_text)
    return pd.DataFrame(data["decisions"])

# ---------------- UI ----------------
st.set_page_config(layout="wide")
st.title("🧠 Planificación Semanal Asistida por IA – Demo Cosmética")

# -------- PROMPT IA FINAL (AQUÍ VIVE LA IA) --------
if "AI_SYSTEM" not in st.session_state:
    st.session_state["AI_SYSTEM"] = """
Eres un ingeniero senior de planificación semanal en la industria cosmética.

Tu función NO es calcular, NO es optimizar y NO es romper restricciones.
Tu función es DECIDIR con criterio humano cuando existe ambigüedad operativa.

REGLAS DURAS (INVIOLABLES):
- Si Estado_Empaque = "BLOQUEADO" → decision = "NO_PRODUCIR".
- Si Demanda_Neta = 0 → decision = "NO_PRODUCIR".
- No inventes datos, fechas ni cantidades.
- No cambies resultados determinísticos.

ZONAS GRISES:
- Si Estado_Empaque = "RIESGO":
  - Puedes decidir "PRODUCIR_CON_RIESGO" si la prioridad implícita y el contexto lo justifican.
  - Puedes decidir "NO_PRODUCIR" si el riesgo operativo supera el beneficio.
- Si Estado_Empaque = "OK":
  - Normalmente decide "PRODUCIR", salvo que exista una razón lógica para no hacerlo.

CRITERIOS A CONSIDERAR (SIN CALCULAR):
- Demanda_Neta vs CEM
- Riesgo de llegada de empaque
- Impacto operativo
- Criterio conservador típico de un ingeniero senior

SALIDA:
Devuelve SOLO JSON válido, sin texto adicional:

{
  "decisions": [
    {
      "sku": "string",
      "decision": "PRODUCIR | PRODUCIR_CON_RIESGO | NO_PRODUCIR",
      "reason": "justificación corta, humana y clara",
      "confidence": 0.0-1.0
    }
  ]
}
""".strip()

# -------- LOAD DATA --------
sheets = load_sheets()

today = st.date_input("Semana de referencia", date.today())
ws, we = week_bounds(today)

if st.button("🚀 Ejecutar planificación semanal"):
    dem = consolidate_orders(sheets["PEDIDOS_CLIENTES"], ws, we)
    net = net_demand(dem, sheets["PRODUCTOS_TERMINADOS"])
    pack = explode_packaging(net, sheets["BOM_EMPAQUE"], sheets["COMPONENTES_EMPAQUE"], we)
    cem = calc_cem(sheets["HISTORIAL_VENTAS"])

    plan = net.merge(pack, on="SKU", how="left").merge(cem, on="SKU", how="left")
    plan["CEM"] = plan["CEM"].fillna(0)

    decisions = ai_decide(plan[[
        "SKU", "Demanda_Neta", "Estado_Empaque", "Detalle_Empaque", "CEM"
    ]])

    out = plan.merge(decisions, on="SKU", how="left")

    st.dataframe(
        out[[
            "SKU",
            "Demanda_Neta",
            "Estado_Empaque",
            "decision",
            "reason",
            "confidence",
        ]],
        use_container_width=True,
    )
