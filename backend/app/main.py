# backend/app/main.py
import asyncio
import json
from typing import Set
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.responses import StreamingResponse

app = FastAPI(title="Landslide Realtime Ingest & SSE")

# Allow development CORS (change in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory set of subscriber queues (dev only). Use Redis pub/sub in prod.
subscribers: Set[asyncio.Queue] = set()

# ----- Pydantic models for validation -----
class Reading(BaseModel):
    station_id: str
    timestamp: str  # ISO 8601
    rainfall_mm_1h: float
    rainfall_mm_24h: float
    rainfall_mm_72h: float
    slope_deg: float | None = None
    soil_moisture: float | None = None

class PredictReq(BaseModel):
    station_id: str
    rainfall_mm_1h: float
    rainfall_mm_24h: float
    rainfall_mm_72h: float

# ----- Simple baseline scoring function (replace with your calibrated ID/ED) -----
def risk_score(r1: float, r24: float, r72: float) -> float:
    """
    Produces a 0..10 score. This is a normalized weighted heuristic:
    tweak weights/normalizers to match your Kerala thresholds.
    """
    # normalizing constants chosen for reasonable ranges; tune later
    s = 0.6 * (r1 / 20.0) + 0.3 * (r24 / 100.0) + 0.1 * (r72 / 300.0)
    s = max(0.0, min(s * 10.0, 10.0))
    return s

def risk_level(score: float) -> str:
    if score < 2.0:
        return "Low"
    if score < 4.5:
        return "Moderate"
    if score < 7.5:
        return "High"
    return "Severe"

# ----- Endpoints -----
@app.post("/ingest")
async def ingest(reading: Reading):
    """
    Accepts new sensor/API reading, scores it, and broadcasts to SSE clients.
    """
    score = risk_score(reading.rainfall_mm_1h, reading.rainfall_mm_24h, reading.rainfall_mm_72h)
    level = risk_level(score)
    payload = {
        "station_id": reading.station_id,
        "timestamp": reading.timestamp,
        "score": round(score, 3),
        "level": level,
        "raw": {
            "rainfall_mm_1h": reading.rainfall_mm_1h,
            "rainfall_mm_24h": reading.rainfall_mm_24h,
            "rainfall_mm_72h": reading.rainfall_mm_72h,
        }
    }
    # Broadcast into all subscriber queues (non-blocking-ish)
    for q in list(subscribers):
        try:
            await q.put(payload)
        except asyncio.QueueFull:
            # if queue is full, we drop this message for that subscriber (tune queue size in prod)
            pass
    return {"ok": True, **payload}

@app.post("/predict")
async def predict(req: PredictReq):
    """
    Synchronous prediction endpoint useful for testing or direct model calls.
    """
    score = risk_score(req.rainfall_mm_1h, req.rainfall_mm_24h, req.rainfall_mm_72h)
    return {"station_id": req.station_id, "score": round(score, 3), "level": risk_level(score)}

@app.get("/stream")
async def stream(request: Request):
    """
    Server-Sent Events endpoint that streams JSON payloads (one event = one JSON object).
    Keeps connection alive by sending a comment ping every 15s when idle.
    """
    async def event_generator():
        q: asyncio.Queue = asyncio.Queue()
        subscribers.add(q)
        try:
            while True:
                # if client disconnected, break and cleanup
                if await request.is_disconnected():
                    break
                try:
                    # wait for new message with timeout to allow heartbeat pings
                    data = await asyncio.wait_for(q.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    # SSE comment to keep connection alive
                    yield ": ping\n\n"
                    continue
                # SSE event: data: <json>\n\n
                yield f"data: {json.dumps(data)}\n\n"
        finally:
            subscribers.discard(q)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# If you run `uvicorn app.main:app --reload` from backend/, this module will serve.
