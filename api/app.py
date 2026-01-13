"""MTA Arrivals Board API - Real-time NYC subway arrivals, weather, and AQI."""

import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
import structlog
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from services import mta, weather, aqi

# Configure structured logging
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    cache_logger_on_first_use=True,
)
log = structlog.get_logger()

http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient()
    log.info("startup")
    yield
    await http_client.aclose()
    log.info("shutdown")


app = FastAPI(title="MTA Arrivals API", version="2.0.0", lifespan=lifespan)


@app.middleware("http")
async def request_context(request: Request, call_next):
    """Add request ID and timing to all requests."""
    request_id = str(uuid.uuid4())[:8]
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)
    
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = int((time.perf_counter() - start) * 1000)
    
    log.info(
        "request",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=duration_ms,
    )
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    errors = [{"field": e["loc"][-1], "type": e["type"]} for e in exc.errors()]
    headers = {k: v for k, v in request.headers.items() if k.startswith(("api", "station", "subway", "lat", "long"))}
    
    log.warning("validation_error", errors=errors, headers=headers)
    
    return JSONResponse(
        status_code=422,
        content={"detail": "Invalid request", "errors": errors, "hint": headers},
    )


def validate_api_key(key: str) -> None:
    expected = os.environ.get("API_KEY")
    if not expected:
        raise HTTPException(500, "API_KEY not configured")
    if key != expected:
        raise HTTPException(401, "Invalid API key")


@app.get("/")
async def health():
    return {"status": "ok"}


@app.get("/api/mta/arrivals")
async def get_arrivals(
    api_key: str = Header(..., alias="api-key"),
    station_ids: str = Header(..., alias="station-ids"),
    subway_lines: str = Header(..., alias="subway-lines"),
    latitude: str = Header(...),
    longitude: str = Header(...),
):
    validate_api_key(api_key)
    
    stations = {s.strip() for s in station_ids.split(",")}
    lines = {l.strip() for l in subway_lines.split(",")}
    
    try:
        lat, lon = float(latitude), float(longitude)
    except ValueError:
        log.error("invalid_coords", latitude=latitude, longitude=longitude)
        raise HTTPException(400, f"Invalid coordinates: {latitude}, {longitude}")
    
    structlog.contextvars.bind_contextvars(stations=list(stations), lines=list(lines))
    
    # Fetch data with timing
    t0 = time.perf_counter()
    arrivals = await mta.get_arrivals(stations, lines, http_client)
    t_arrivals = time.perf_counter()
    
    alerts = await mta.get_alerts(lines, http_client)
    t_alerts = time.perf_counter()
    
    weather_data = await weather.get_weather(lat, lon, http_client)
    t_weather = time.perf_counter()
    
    aqi_data = await aqi.get_aqi(lat, lon, http_client)
    t_aqi = time.perf_counter()
    
    response = {**arrivals, "alerts": alerts}
    
    if weather_data:
        response["weather"] = {
            "temp_f": weather_data.temp_f,
            "feels_like_f": weather_data.feels_like_f,
            "conditions": weather_data.conditions,
        }
    
    if aqi_data:
        response["aqi"] = {"value": aqi_data.value, "level": aqi_data.level}
    
    log.info(
        "fulfilled",
        spans={
            "mta_ms": int((t_arrivals - t0) * 1000),
            "alerts_ms": int((t_alerts - t_arrivals) * 1000),
            "weather_ms": int((t_weather - t_alerts) * 1000),
            "aqi_ms": int((t_aqi - t_weather) * 1000),
        },
    )
    
    return response
