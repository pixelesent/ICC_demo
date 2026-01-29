from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import date
import time

app = FastAPI(
    title="ICC Demo – Motor de Planificación",
    version="0.2-debug"
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

    # ⛔ NO SE USAN AÚN (pero se reciben)
    materias_primas: List[Dict[str, Any]]
    formula_mp: List[Dict[str, Any]]
    mezcladoras: List[Dict[str, Any]]
    llenadoras: List[Dict[str, Any]]


# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def index_by_key(rows, key):
    return {r[key]: r for r in rows if key in r}


# -------------------------------------------------
# CORE LOGIC – PRODUCCIÓN (DEBUG)
# -------------------------------------------------

def calcular_demanda_neta(demanda, productos_terminados):
    print("▶️ calcular_demanda_neta()")
    t0 = time.time()

    pt_index = index_by_key(productos_terminados, "SKU")
    out = []

    for d in demanda:
        print(f"   • SKU {d.SKU} | demanda_bruta={d.demanda_bruta}")
        inv = int(pt_index.get(d.SKU, {}).get("Inventario", 0))
        neta = max(0, d.demanda_bruta - inv)

        out.append({
            "SKU": d.SKU,
            "Demanda_Bruta": d.demanda_bruta,
            "Inventario_PT": inv,
            "Demanda_Neta": neta
        })

    print(f"✅ Demanda neta lista ({round(time.time() - t0, 2)}s)")
    return out


def explosion_empaque(demanda_neta, bom, componentes):
    print("▶️ explosion_empaque()")
    t0 = time.time()

    comp_index = index_by_key(componentes, "Componente_ID")
    resultado = []

    for sku_row in demanda_neta:
        sku = sku_row["SKU"]
        print(f"   • Explosión empaque SKU {sku}")

        if sku_row["Demanda_Neta"] <= 0:
            sku_row["Estado_Empaque"] = "OK"
            sku_row["Detalle_Empaque"] = []
            resultado.append(sku_row)
            continue

        componentes_sku = [b for b in bom if b.get("SKU") == sku]

        estados = []
        detalles = []

        for b in componentes_sku:
            comp_id = b.get("COMPONENTE_ID")
            comp = comp_index.get(comp_id)

            if not comp:
                estados.append("BLOQUEADO")
                detalles.append(comp_id)
                continue

            requerido = float(b.get("CANTIDAD_POR_UNIDAD", 0)) * sku_row["Demanda_Neta"]
            inv = float(comp.get("Inventario", 0))
            en_proceso = float(comp.get("En_Proceso", 0))

            if inv >= requerido:
                estado = "OK"
            elif inv + en_proceso >= requerido:
                estado = "RIESGO"
            else:
                estado = "BLOQUEADO"

            estados.append(estado)
            if estado != "OK":
                detalles.append(comp_id)

        if "BLOQUEADO" in estados:
            sku_row["Estado_Empaque"] = "BLOQUEADO"
        elif "RIESGO" in estados:
            sku_row["Estado_Empaque"] = "RIESGO"
        else:
            sku_row["Estado_Empaque"] = "OK"

        sku_row["Detalle_Empaque"] = detalles
        resultado.append(sku_row)

    print(f"✅ Explosión empaque lista ({round(time.time() - t0, 2)}s)")
    return resultado


# -------------------------------------------------
# ENDPOINT PRINCIPAL (MODO DEBUG)
# -------------------------------------------------

@app.post("/planificacion/semanal")
def planificacion_semanal(data: BackendInput):

    print("🚀 /planificacion/semanal START")
    t_total = time.time()

    # 🔴 CORTE EXPLÍCITO A 1 SKU
    demanda_test = data.demanda[:1]
    print(f"⚠️ MODO TEST: procesando {len(demanda_test)} SKU")

    # 1️⃣ Demanda neta
    demanda_neta = calcular_demanda_neta(
        demanda_test,
        data.productos_terminados
    )

    # 2️⃣ Explosión de empaque
    produccion = explosion_empaque(
        demanda_neta,
        data.bom_empaque,
        data.componentes_empaque
    )

    print(f"⏱️ TOTAL {round(time.time() - t_total, 2)}s")

    # ⛔ MP / Mezcladoras / Llenadoras DESACTIVADAS
    return {
        "modo": "DEBUG_1_SKU",
        "week_start": data.week_start,
        "week_end": data.week_end,
        "resultado": produccion
    }


# -------------------------------------------------
# ENDPOINT DRY-RUN (NO TOCADO)
# -------------------------------------------------

@app.post("/planificacion/dry-run")
def planificacion_dry_run(data: BackendInput):

    skus_demandados = {d.SKU for d in data.demanda}
    skus_con_bom = {b["SKU"] for b in data.bom_empaque if "SKU" in b}
    skus_con_formula = {f["SKU"] for f in data.formula_mp if "SKU" in f}

    familias_por_sku = {}
    for p in data.productos_terminados:
        if p.get("SKU"):
            familias_por_sku[p["SKU"]] = p.get("Familia")

    familias_mezcladoras = {
        m.get("Familias_compatibles") for m in data.mezcladoras if m.get("Familias_compatibles")
    }
    familias_llenadoras = {
        l.get("Familias_compatibles") for l in data.llenadoras if l.get("Familias_compatibles")
    }

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
