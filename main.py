import os
import logging
import asyncio
from datetime import date, datetime, timezone
from typing import List, Dict, Any
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from supabase import create_client, Client

# ----------------------------
# LOGGING (visible en Render)
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("icc-demo")

# ----------------------------
# SUPABASE CLIENT
# ----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ----------------------------
# FASTAPI
# ----------------------------
app = FastAPI(title="ICC Demo – Motor de Planificación", version="1.0")

# ----------------------------
# INPUT MODELS (Make -> Backend)
# ----------------------------
class DemandaSKU(BaseModel):
    SKU: str = Field(..., min_length=1)
    demanda_bruta: int = Field(..., ge=0)

class BackendInputMin(BaseModel):
    week_start: date
    week_end: date
    demanda: List[DemandaSKU]

# ----------------------------
# HELPERS
# ----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def index_by_key(rows: List[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        if key in r and r[key] is not None:
            out[str(r[key])] = r
    return out

def safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# ----------------------------
# READ STATIC DATA FROM SUPABASE
# ----------------------------
def fetch_table_all(table_name: str) -> List[Dict[str, Any]]:
    resp = sb.table(table_name).select("*").execute()
    return resp.data or []

def fetch_static_data() -> Dict[str, Any]:
    log.info("DB: fetching static tables from Supabase...")

    # Nombres EXACTOS en Supabase:
    productos_terminados = fetch_table_all("productos_terminados")
    componentes_empaque  = fetch_table_all("componentes_empaque")
    materias_primas      = fetch_table_all("materias_primas")
    bom_empaque          = fetch_table_all("bom_empaque")
    formula_mp           = fetch_table_all("formula_mp")
    historial_ventas     = fetch_table_all("historial_ventas")
    mezcladoras          = fetch_table_all("mezcladoras")
    llenadoras           = fetch_table_all("llenadoras")

    log.info(
        "DB: loaded | PT=%s | COMP=%s | MP=%s | BOM=%s | FORM=%s | VENTAS=%s | MEZ=%s | LLEN=%s",
        len(productos_terminados),
        len(componentes_empaque),
        len(materias_primas),
        len(bom_empaque),
        len(formula_mp),
        len(historial_ventas),
        len(mezcladoras),
        len(llenadoras),
    )

    return {
        "productos_terminados": productos_terminados,
        "componentes_empaque": componentes_empaque,
        "materias_primas": materias_primas,
        "bom_empaque": bom_empaque,
        "formula_mp": formula_mp,
        "historial_ventas": historial_ventas,
        "mezcladoras": mezcladoras,
        "llenadoras": llenadoras,
    }

# ----------------------------
# CORE CALCS (REAL, pero enfocado a demo)
# ----------------------------
def calcular_demanda_neta(demanda: List[Dict[str, Any]], productos_terminados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pt_index = index_by_key(productos_terminados, "SKU")
    out: List[Dict[str, Any]] = []

    for d in demanda or []:
        sku = str(d.get("SKU", "")).strip()
        inv = safe_int(pt_index.get(sku, {}).get("Inventario", 0))
        bruta = safe_int(d.get("demanda_bruta", 0))
        neta = max(0, bruta - inv)

        out.append({
            "SKU": sku,
            "Demanda_Bruta": bruta,
            "Inventario_PT": inv,
            "Demanda_Neta": neta
        })
    return out

def explosion_empaque(demanda_neta: List[Dict[str, Any]], bom_empaque: List[Dict[str, Any]], componentes_empaque: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Ajuste de key: en tu Google Sheet y backend viejo aparece "Componente_ID"
    # pero en BOM aparece "COMPONENTE_ID". Cubrimos ambos.
    comp_index = index_by_key(componentes_empaque, "Componente_ID")

    bom_por_sku: Dict[str, List[Dict[str, Any]]] = {}
    for b in bom_empaque or []:
        sku = str(b.get("SKU", "")).strip()
        if sku:
            bom_por_sku.setdefault(sku, []).append(b)

    resultado: List[Dict[str, Any]] = []
    for row in demanda_neta or []:
        sku = str(row.get("SKU", "")).strip()
        neta = safe_int(row.get("Demanda_Neta", 0), 0)

        if neta <= 0:
            row["Estado_Empaque"] = "OK"
            row["Detalle_Empaque"] = []
            resultado.append(row)
            continue

        componentes_sku = bom_por_sku.get(sku, [])
        estados: List[str] = []
        detalles: List[str] = []

        for b in componentes_sku:
            comp_id = str(b.get("COMPONENTE_ID") or b.get("Componente_ID") or "").strip()
            if not comp_id:
                estados.append("BLOQUEADO")
                detalles.append("COMPONENTE_ID_MISSING")
                continue

            comp = comp_index.get(comp_id)
            if not comp:
                estados.append("BLOQUEADO")
                detalles.append(comp_id)
                continue

            cant = safe_float(b.get("CANTIDAD_POR_UNIDAD"), 0.0)
            requerido = cant * neta

            inv = safe_float(comp.get("Inventario", 0))
            en_proceso = safe_float(comp.get("En_Proceso", 0))

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
            row["Estado_Empaque"] = "BLOQUEADO"
        elif "RIESGO" in estados:
            row["Estado_Empaque"] = "RIESGO"
        else:
            row["Estado_Empaque"] = "OK"

        row["Detalle_Empaque"] = detalles
        resultado.append(row)

    return resultado

def build_result_payload(week_start: date, week_end: date, demanda_neta: List[Dict[str, Any]], empaque: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "week": {"start": str(week_start), "end": str(week_end)},
        "demanda_neta": demanda_neta,
        "empaque": empaque,
        "nota": "Cálculo real: demanda neta + explosión de empaque. MP/mezcladoras/llenadoras se activan después."
    }

# ----------------------------
# JOB WORKER (BACKGROUND)
# ----------------------------
async def process_job(job_id: str):
    log.info("JOB %s | start", job_id)
    try:
        # marcar processing + timestamps (sin now() server-side para evitar problemas)
        sb.table("planificacion_jobs").update({
            "status": "processing",
            "started_at": utc_now_iso()
        }).eq("job_id", job_id).execute()

        # leer job
        job = sb.table("planificacion_jobs").select("*").eq("job_id", job_id).single().execute().data
        if not job:
            raise RuntimeError("job not found")

        week_start = date.fromisoformat(job["week_start"])
        week_end = date.fromisoformat(job["week_end"])
        demanda = job.get("demanda") or []

        log.info("JOB %s | input | week=%s..%s | demanda_items=%s", job_id, week_start, week_end, len(demanda))

        # 1) cargar estáticos desde Supabase (thread para no bloquear)
        static = await asyncio.to_thread(fetch_static_data)

        # 2) cálculos
        demanda_neta = await asyncio.to_thread(calcular_demanda_neta, demanda, static["productos_terminados"])
        empaque = await asyncio.to_thread(explosion_empaque, demanda_neta, static["bom_empaque"], static["componentes_empaque"])
        resultado = await asyncio.to_thread(build_result_payload, week_start, week_end, demanda_neta, empaque)

        # 3) guardar resultado
        sb.table("planificacion_resultados").upsert({
            "job_id": job_id,
            "resultado": resultado,
            "updated_at": utc_now_iso()
        }).execute()

        # marcar done
        sb.table("planificacion_jobs").update({
            "status": "done",
            "finished_at": utc_now_iso(),
            "error_message": None
        }).eq("job_id", job_id).execute()

        log.info("JOB %s | done", job_id)

    except Exception as e:
        log.exception("JOB %s | error", job_id)
        sb.table("planificacion_jobs").update({
            "status": "error",
            "finished_at": utc_now_iso(),
            "error_message": str(e)
        }).eq("job_id", job_id).execute()

# ----------------------------
# API ENDPOINTS
# ----------------------------
@app.post("/planificacion/semanal")
async def planificacion_semanal(payload: BackendInputMin):
    # 1) crear job_id en backend (NO dependemos de returning)
    job_id = str(uuid4())

    demanda_list = [{"SKU": d.SKU, "demanda_bruta": d.demanda_bruta} for d in payload.demanda]

    sb.table("planificacion_jobs").insert({
        "job_id": job_id,
        "week_start": str(payload.week_start),
        "week_end": str(payload.week_end),
        "demanda": demanda_list,
        "status": "queued",
        "created_at": utc_now_iso()
    }).execute()

    log.info("API: created job %s | demand_items=%s", job_id, len(demanda_list))

    # 2) background task (no bloquea el request)
    asyncio.create_task(process_job(job_id))

    # 3) responder inmediato
    return {"job_id": job_id, "status": "queued"}

@app.get("/planificacion/resultado/{job_id}")
def planificacion_resultado(job_id: UUID):
    job = sb.table("planificacion_jobs") \
        .select("job_id,status,error_message,week_start,week_end,created_at,started_at,finished_at") \
        .eq("job_id", str(job_id)) \
        .single() \
        .execute() \
        .data

    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    status = job.get("status")

    if status == "done":
        res = sb.table("planificacion_resultados") \
            .select("resultado") \
            .eq("job_id", str(job_id)) \
            .single() \
            .execute() \
            .data
        return {"job": job, "resultado": (res or {}).get("resultado")}

    return {"job": job}
