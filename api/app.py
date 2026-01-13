"""
MTA Arrivals Board API

A FastAPI application that provides real-time NYC subway arrivals,
weather, and air quality data for LED matrix displays.

Endpoints:
    GET /api/mta/arrivals - Subway arrivals with weather and AQI

Environment Variables:
    API_KEY - Secret key for authenticating requests
"""

import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Header, HTTPException

from services import mta, weather, aqi


# Shared HTTP client for all outbound requests
http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage shared resources across the application lifecycle."""
    global http_client
    http_client = httpx.AsyncClient()
    yield
    await http_client.aclose()


app = FastAPI(
    title="MTA Arrivals Board API",
    description="Real-time NYC subway arrivals, weather, and air quality for LED displays",
    version="2.0.0",
    lifespan=lifespan,
)


def validate_api_key(provided_key: str) -> None:
    """Raise 401 if the provided API key doesn't match the configured key."""
    expected_key = os.environ.get("API_KEY")
    if not expected_key:
        raise HTTPException(status_code=500, detail="API_KEY not configured")
    if provided_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "mta-arrivals-api"}


@app.get("/api/mta/arrivals")
async def get_mta_arrivals(
    api_key: str = Header(..., alias="api-key"),
    station_ids: str = Header(..., alias="station-ids"),
    subway_lines: str = Header(..., alias="subway-lines"),
    latitude: float = Header(...),
    longitude: float = Header(...),
):
    """
    Get MTA subway arrivals with weather and air quality data.

    Headers:
        api-key: Your API key
        station-ids: Comma-separated station IDs (e.g., "A32N,A32S")
        subway-lines: Comma-separated lines (e.g., "A,C,E")
        latitude: Location latitude for weather/AQI
        longitude: Location longitude for weather/AQI

    Returns:
        JSON with North/South arrivals, alerts, weather, and AQI data.
    """
    validate_api_key(api_key)

    parsed_station_ids = {s.strip() for s in station_ids.split(",")}
    parsed_lines = {l.strip() for l in subway_lines.split(",")}

    # Fetch all data concurrently
    arrivals_data = await mta.get_arrivals(
        station_ids=parsed_station_ids,
        lines=parsed_lines,
        client=http_client,
    )

    alerts_data = await mta.get_alerts(
        lines=parsed_lines,
        client=http_client,
    )

    weather_data = await weather.get_weather(
        lat=latitude,
        lon=longitude,
        client=http_client,
    )

    aqi_data = await aqi.get_aqi(
        lat=latitude,
        lon=longitude,
        client=http_client,
    )

    # Build response (backward compatible with existing firmware)
    response = {
        **arrivals_data,
        "alerts": alerts_data,
    }

    # Add weather if available
    if weather_data:
        response["weather"] = {
            "temp_f": weather_data.temp_f,
            "feels_like_f": weather_data.feels_like_f,
            "conditions": weather_data.conditions,
        }

    # Add AQI if available
    if aqi_data:
        response["aqi"] = {
            "value": aqi_data.value,
            "level": aqi_data.level,
        }

    return response
