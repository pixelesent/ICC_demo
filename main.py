import os
import logging
import asyncio
from datetime import date, datetime, timezone
from typing import List, Dict, Any
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from supabase import create_client, Client

# ----------------------------
# LOGGING
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("icc-demo")

# ----------------------------
# SUPABASE
# ----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ----------------------------
# MAKE WEBHOOK (DEMO)
# ----------------------------
MAKE_JOB_DONE_WEBHOOK = "https://hook.us2.make.com/ayd47wm9xit4kihxdkrskiiva1p3xwzd"

# ----------------------------
# FASTAPI
# ----------------------------
app = FastAPI(title="ICC Demo – Motor de Planificación", version="1.0")

# ----------------------------
# INPUT MODELS
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
    return {str(r[key]): r for r in rows or [] if key in r and r[key] is not None}

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
# DB READS
# ----------------------------
def fetch_table_all(table_name: str) -> List[Dict[str, Any]]:
    return sb.table(table_name).select("*").execute().data or []

def fetch_static_data() -> Dict[str, Any]:
    log.info("DB: loading static tables")
    return {
        "productos_terminados": fetch_table_all("productos_terminados"),
        "componentes_empaque": fetch_table_all("componentes_empaque"),
        "materias_primas": fetch_table_all("materias_primas"),
        "bom_empaque": fetch_table_all("bom_empaque"),
        "formula_mp": fetch_table_all("formula_mp"),
        "historial_ventas": fetch_table_all("historial_ventas"),
        "mezcladoras": fetch_table_all("mezcladoras"),
        "llenadoras": fetch_table_all("llenadoras"),
    }

# ----------------------------
# CORE LOGIC
# ----------------------------
def calcular_demanda_neta(demanda, productos_terminados):
    pt_index = index_by_key(productos_terminados, "SKU")
    out = []

    for d in demanda:
        sku = d["SKU"]
        inv = safe_int(pt_index.get(sku, {}).get("Inventario", 0))
        bruta = safe_int(d["demanda_bruta"])
        out.append({
            "SKU": sku,
            "Demanda_Bruta": bruta,
            "Inventario_PT": inv,
            "Demanda_Neta": max(0, bruta - inv)
        })
    return out

def explosion_empaque(demanda_neta, bom_empaque, componentes_empaque):
    comp_index = index_by_key(componentes_empaque, "Componente_ID")
    bom_por_sku = {}

    for b in bom_empaque:
        bom_por_sku.setdefault(b["SKU"], []).append(b)

    resultado = []
    for row in demanda_neta:
        estados, detalles = [], []
        for b in bom_por_sku.get(row["SKU"], []):
            cid = b.get("COMPONENTE_ID") or b.get("Componente_ID")
            comp = comp_index.get(cid)
            requerido = safe_float(b.get("CANTIDAD_POR_UNIDAD")) * row["Demanda_Neta"]

            inv = safe_float(comp.get("Inventario", 0)) if comp else 0
            proc = safe_float(comp.get("En_Proceso", 0)) if comp else 0

            if inv >= requerido:
                estado = "OK"
            elif inv + proc >= requerido:
                estado = "RIESGO"
            else:
                estado = "BLOQUEADO"

            estados.append(estado)
            if estado != "OK":
                detalles.append(cid)

        row["Estado_Empaque"] = (
            "BLOQUEADO" if "BLOQUEADO" in estados
            else "RIESGO" if "RIESGO" in estados
            else "OK"
        )
        row["Detalle_Empaque"] = detalles
        resultado.append(row)

    return resultado

def build_result_payload(ws, we, demanda_neta, empaque):
    return {
        "week": {"start": str(ws), "end": str(we)},
        "demanda_neta": demanda_neta,
        "empaque": empaque,
        "nota": "Cálculo real: demanda neta + explosión de empaque."
    }

# ----------------------------
# BACKGROUND JOB
# ----------------------------
async def process_job(job_id: str):
    try:
        sb.table("planificacion_jobs").update({
            "status": "processing",
            "started_at": utc_now_iso()
        }).eq("job_id", job_id).execute()

        job = sb.table("planificacion_jobs").select("*").eq("job_id", job_id).single().execute().data
        static = await asyncio.to_thread(fetch_static_data)

        demanda_neta = await asyncio.to_thread(
            calcular_demanda_neta, job["demanda"], static["productos_terminados"]
        )
        empaque = await asyncio.to_thread(
            explosion_empaque, demanda_neta, static["bom_empaque"], static["componentes_empaque"]
        )

        resultado = build_result_payload(job["week_start"], job["week_end"], demanda_neta, empaque)

        sb.table("planificacion_resultados").upsert({
            "job_id": job_id,
            "resultado": resultado,
            "updated_at": utc_now_iso()
        }).execute()

        sb.table("planificacion_jobs").update({
            "status": "done",
            "finished_at": utc_now_iso()
        }).eq("job_id", job_id).execute()

        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(
                MAKE_JOB_DONE_WEBHOOK,
                json={"job_id": job_id, "status": "done"}
            )

    except Exception as e:
        sb.table("planificacion_jobs").update({
            "status": "error",
            "finished_at": utc_now_iso(),
            "error_message": str(e)
        }).eq("job_id", job_id).execute()

# ----------------------------
# API
# ----------------------------
@app.post("/planificacion/semanal")
async def planificacion_semanal(payload: BackendInputMin):
    job_id = str(uuid4())
    sb.table("planificacion_jobs").insert({
        "job_id": job_id,
        "week_start": str(payload.week_start),
        "week_end": str(payload.week_end),
        "demanda": [{"SKU": d.SKU, "demanda_bruta": d.demanda_bruta} for d in payload.demanda],
        "status": "queued",
        "created_at": utc_now_iso()
    }).execute()

    asyncio.create_task(process_job(job_id))
    return {"job_id": job_id, "status": "queued"}

@app.get("/planificacion/resultado/{job_id}")
def planificacion_resultado(job_id: UUID):
    job = sb.table("planificacion_jobs").select("*").eq("job_id", str(job_id)).single().execute().data
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    if job["status"] == "done":
        res = sb.table("planificacion_resultados").select("resultado").eq("job_id", str(job_id)).single().execute().data
        return {"job": job, "resultado": res["resultado"]}

    return {"job": job}
