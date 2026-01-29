from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import date
from math import ceil

app = FastAPI(
    title="ICC Demo – Motor de Planificación",
    version="0.1"
)

# -------------------------------------------------
# MODELOS DE INPUT (Make → Backend)
# -------------------------------------------------

class DemandaSKU(BaseModel):
    SKU: str
    demanda_bruta: int


class BackendInput(BaseModel):
    week_start: date
    week_end: date

    demanda: List[DemandaSKU]

    productos_terminados: List[Dict[str, Any]]
    componentes_empaque: List[Dict[str, Any]]
    bom_empaque: List[Dict[str, Any]]
    materias_primas: List[Dict[str, Any]]
    formula_mp: List[Dict[str, Any]]
    mezcladoras: List[Dict[str, Any]]
    llenadoras: List[Dict[str, Any]]


# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def index_by_key(rows, key):
    return {r[key]: r for r in rows}


# -------------------------------------------------
# CORE LOGIC – PRODUCCIÓN
# -------------------------------------------------

def calcular_demanda_neta(demanda, productos_terminados):
    pt_index = index_by_key(productos_terminados, "SKU")

    out = []
    for d in demanda:
        inv = int(pt_index.get(d.SKU, {}).get("Inventario", 0))
        neta = max(0, d.demanda_bruta - inv)

        out.append({
            "SKU": d.SKU,
            "Demanda_Bruta": d.demanda_bruta,
            "Inventario_PT": inv,
            "Demanda_Neta": neta
        })

    return out


def explosion_empaque(demanda_neta, bom, componentes, week_end):
    comp_index = index_by_key(componentes, "Componente_ID")

    resultado = []

    for sku_row in demanda_neta:
        if sku_row["Demanda_Neta"] <= 0:
            sku_row["Estado_Empaque"] = "OK"
            sku_row["Detalle_Empaque"] = []
            resultado.append(sku_row)
            continue

        sku = sku_row["SKU"]
        componentes_sku = [b for b in bom if b["SKU"] == sku]

        estados = []
        detalles = []

        for b in componentes_sku:
            comp = comp_index.get(b["COMPONENTE_ID"])
            if not comp:
                estados.append("BLOQUEADO")
                detalles.append(b["COMPONENTE_ID"])
                continue

            requerido = float(b["CANTIDAD_POR_UNIDAD"]) * sku_row["Demanda_Neta"]
            inv = float(comp.get("Inventario", 0))
            en_proceso = float(comp.get("En_Proceso", 0))
            fecha = comp.get("Fecha_Estimada")

            if inv >= requerido:
                estado = "OK"
            elif inv + en_proceso >= requerido:
                estado = "RIESGO"
            else:
                estado = "BLOQUEADO"

            estados.append(estado)
            if estado != "OK":
                detalles.append(b["COMPONENTE_ID"])

        if "BLOQUEADO" in estados:
            sku_row["Estado_Empaque"] = "BLOQUEADO"
        elif "RIESGO" in estados:
            sku_row["Estado_Empaque"] = "RIESGO"
        else:
            sku_row["Estado_Empaque"] = "OK"

        sku_row["Detalle_Empaque"] = detalles
        resultado.append(sku_row)

    return resultado


# -------------------------------------------------
# ENDPOINT PRINCIPAL
# -------------------------------------------------

@app.post("/planificacion/semanal")
def planificacion_semanal(data: BackendInput):

    # 1️⃣ Demanda neta
    demanda_neta = calcular_demanda_neta(
        data.demanda,
        data.productos_terminados
    )

    # 2️⃣ Explosión de empaque
    produccion = explosion_empaque(
        demanda_neta,
        data.bom_empaque,
        data.componentes_empaque,
        data.week_end
    )

    # 3️⃣ Resultado base (IA viene después)
    return {
        "week_start": data.week_start,
        "week_end": data.week_end,
        "resultado": produccion
    }
