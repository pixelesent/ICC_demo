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

# -------------------------------------------------
# ENDPOINT DRY-RUN (VALIDACIÓN ESTRUCTURAL)
# -------------------------------------------------

@app.post("/planificacion/dry-run")
def planificacion_dry_run(data: BackendInput):

    # SKUs demandados
    skus_demandados = {d.SKU for d in data.demanda}

    # Índices básicos
    skus_con_bom = {b["SKU"] for b in data.bom_empaque if "SKU" in b}
    skus_con_formula = {f["SKU"] for f in data.formula_mp if "SKU" in f}

    # Familias por SKU
    familias_por_sku = {}
    for p in data.productos_terminados:
        sku = p.get("SKU")
        fam = p.get("Familia")
        if sku:
            familias_por_sku[sku] = fam

    # Familias compatibles
    familias_mezcladoras = set()
    for m in data.mezcladoras:
        fam = m.get("Familias_compatibles")
        if fam:
            familias_mezcladoras.add(fam)

    familias_llenadoras = set()
    for l in data.llenadoras:
        fam = l.get("Familias_compatibles")
        if fam:
            familias_llenadoras.add(fam)

    # Validaciones
    skus_sin_bom = list(skus_demandados - skus_con_bom)
    skus_sin_formula = list(skus_demandados - skus_con_formula)

    skus_sin_mezcladora = []
    skus_sin_llenadora = []

    for sku in skus_demandados:
        fam = familias_por_sku.get(sku)
        if fam and fam not in familias_mezcladoras:
            skus_sin_mezcladora.append(sku)
        if fam and fam not in familias_llenadoras:
            skus_sin_llenadora.append(sku)

    return {
        "status": "ok",
        "modo": "dry-run",
        "week": {
            "start": data.week_start,
            "end": data.week_end
        },
        "resumen": {
            "skus_demandados": len(skus_demandados),
            "skus_sin_bom_empaque": skus_sin_bom,
            "skus_sin_formula_mp": skus_sin_formula,
            "skus_sin_mezcladora_compatible": skus_sin_mezcladora,
            "skus_sin_llenadora_compatible": skus_sin_llenadora,
            "mezcladoras_disponibles": len(data.mezcladoras),
            "llenadoras_disponibles": len(data.llenadoras)
        },
        "nota": "Dry-run activo. No se ejecutaron cálculos de producción."
    }
