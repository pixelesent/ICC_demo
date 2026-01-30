from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import date
import logging
import time

# -------------------------------------------------
# APP + LOGGING
# -------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("ICC-DEMO")

app = FastAPI(
    title="ICC Demo – Motor de Planificación (SAFE DEMO)",
    version="0.2-demo-safe"
)

# -------------------------------------------------
# MODELOS
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

    # ⚠️ se reciben pero NO se procesan en demo
    materias_primas: List[Dict[str, Any]]
    formula_mp: List[Dict[str, Any]]
    mezcladoras: List[Dict[str, Any]]
    llenadoras: List[Dict[str, Any]]

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def index_by_key(rows, key):
    return {r.get(key): r for r in rows if key in r}

# -------------------------------------------------
# ENDPOINT PRINCIPAL (ANTI-TIMEOUT)
# -------------------------------------------------

@app.post("/planificacion/semanal")
def planificacion_semanal(data: BackendInput):

    start_ts = time.time()
    log.info("=== INICIO PLANIFICACION SEMANAL (DEMO SAFE) ===")

    # -------------------------------
    # 1. LIMITES DE DEMO
    # -------------------------------
    MAX_SKUS = 5        # 🔥 LIMITE DURO
    MAX_SECONDS = 8.0   # 🔥 LIMITE DE TIEMPO

    # -------------------------------
    # 2. INDEXES MINIMOS
    # -------------------------------
    log.info("Indexando productos terminados y componentes...")
    pt_index = index_by_key(data.productos_terminados, "SKU")
    comp_index = index_by_key(data.componentes_empaque, "Componente_ID")

    bom_por_sku = {}
    for b in data.bom_empaque:
        sku = b.get("SKU")
        if sku:
            bom_por_sku.setdefault(sku, []).append(b)

    # -------------------------------
    # 3. LOOP CONTROLADO POR SKU
    # -------------------------------
    resultado = []
    skus_procesados = 0

    for d in data.demanda:

        # ⏱️ CORTE POR TIEMPO
        if time.time() - start_ts > MAX_SECONDS:
            log.warning("⏱️ Corte por tiempo alcanzado")
            break

        # 🔢 CORTE POR CANTIDAD
        if skus_procesados >= MAX_SKUS:
            log.warning("🔢 Corte por limite de SKUs")
            break

        sku = d.SKU
        demanda_bruta = d.demanda_bruta

        log.info(f"Procesando SKU {sku}")

        inv_pt = int(pt_index.get(sku, {}).get("Inventario", 0))
        demanda_neta = max(0, demanda_bruta - inv_pt)

        estado_empaque = "OK"
        detalle_empaque = []

        if demanda_neta > 0:
            for b in bom_por_sku.get(sku, []):
                comp = comp_index.get(b.get("COMPONENTE_ID"))
                if not comp:
                    estado_empaque = "BLOQUEADO"
                    detalle_empaque.append(b.get("COMPONENTE_ID"))
                    continue

                requerido = float(b.get("CANTIDAD_POR_UNIDAD", 0)) * demanda_neta
                inv = float(comp.get("Inventario", 0))
                en_proceso = float(comp.get("En_Proceso", 0))

                if inv >= requerido:
                    continue
                elif inv + en_proceso >= requerido:
                    estado_empaque = "RIESGO"
                    detalle_empaque.append(b.get("COMPONENTE_ID"))
                else:
                    estado_empaque = "BLOQUEADO"
                    detalle_empaque.append(b.get("COMPONENTE_ID"))

        resultado.append({
            "SKU": sku,
            "Demanda_Bruta": demanda_bruta,
            "Inventario_PT": inv_pt,
            "Demanda_Neta": demanda_neta,
            "Estado_Empaque": estado_empaque,
            "Detalle_Empaque": detalle_empaque
        })

        skus_procesados += 1

    # -------------------------------
    # 4. RESPUESTA FINAL (RAPIDA)
    # -------------------------------
    elapsed = round(time.time() - start_ts, 2)
    log.info(f"=== FIN DEMO | {skus_procesados} SKUs | {elapsed}s ===")

    return {
        "modo": "demo-safe",
        "week_start": data.week_start,
        "week_end": data.week_end,
        "skus_procesados": skus_procesados,
        "tiempo_segundos": elapsed,
        "resultado": resultado,
        "nota": "Procesamiento limitado para evitar timeout. Mezcladoras, llenadoras y MP desactivadas."
    }
