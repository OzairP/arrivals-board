"""
National Weather Service (NWS) weather data service.

Free, no API key required. US locations only.
https://www.weather.gov/documentation/services-web-api
"""

from dataclasses import dataclass

import httpx


@dataclass
class Weather:
    temp_f: int
    feels_like_f: int | None
    conditions: str


async def get_weather(lat: float, lon: float, client: httpx.AsyncClient) -> Weather | None:
    """
    Fetch current weather from the National Weather Service.

    Args:
        lat: Latitude (decimal degrees)
        lon: Longitude (decimal degrees)
        client: httpx async client for making requests

    Returns:
        Weather dataclass with temperature and conditions, or None on error.

    NWS API flow:
        1. GET /points/{lat},{lon} → returns forecast office + grid coordinates
        2. GET /gridpoints/{office}/{grid_x},{grid_y}/forecast/hourly → returns forecast
    """
    try:
        # Step 1: Get the grid point for this location
        points_url = f"https://api.weather.gov/points/{lat},{lon}"
        headers = {"User-Agent": "mta-arrivals-board (github.com/benarnav/arrivals-board)"}

        points_response = await client.get(points_url, headers=headers, timeout=10.0)
        points_response.raise_for_status()
        points_data = points_response.json()

        forecast_url = points_data["properties"]["forecastHourly"]

        # Step 2: Get the hourly forecast
        forecast_response = await client.get(forecast_url, headers=headers, timeout=10.0)
        forecast_response.raise_for_status()
        forecast_data = forecast_response.json()

        # Current conditions are the first period
        current = forecast_data["properties"]["periods"][0]

        return Weather(
            temp_f=current["temperature"],
            feels_like_f=None,  # NWS doesn't provide feels-like in hourly
            conditions=current["shortForecast"],
        )

    except (httpx.RequestError, httpx.HTTPStatusError, KeyError, IndexError):
        return None
