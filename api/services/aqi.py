"""
Air Quality Index (AQI) service using Open-Meteo.

Free, no API key required, global coverage.
https://open-meteo.com/en/docs/air-quality-api
"""

from dataclasses import dataclass

import httpx


@dataclass
class AirQuality:
    value: int
    level: int  # 1-6 matching firmware's aqi_lvl index


def _aqi_to_level(aqi: int) -> int:
    """
    Convert US AQI value to level index (1-6).
    Matches the firmware's aqi_sheet.bmp tile indices.
    """
    if aqi <= 50:
        return 1  # Good
    elif aqi <= 100:
        return 2  # Moderate
    elif aqi <= 150:
        return 3  # Unhealthy for Sensitive Groups
    elif aqi <= 200:
        return 4  # Unhealthy
    elif aqi <= 300:
        return 5  # Very Unhealthy
    else:
        return 6  # Hazardous


async def get_aqi(lat: float, lon: float, client: httpx.AsyncClient) -> AirQuality | None:
    """
    Fetch current Air Quality Index from Open-Meteo.

    Args:
        lat: Latitude (decimal degrees)
        lon: Longitude (decimal degrees)
        client: httpx async client for making requests

    Returns:
        AirQuality dataclass with US AQI value and level, or None on error.
    """
    try:
        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "us_aqi",
        }

        response = await client.get(url, params=params, timeout=10.0)
        response.raise_for_status()
        data = response.json()

        aqi_value = int(data["current"]["us_aqi"])

        return AirQuality(
            value=aqi_value,
            level=_aqi_to_level(aqi_value),
        )

    except (httpx.RequestError, httpx.HTTPStatusError, KeyError, ValueError):
        return None
