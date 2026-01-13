"""
MTA (NYC Subway) arrivals and alerts service.

Uses the MTA's public GTFS-realtime feeds to fetch train arrival predictions.
No API key required.
"""

import asyncio
import time
from dataclasses import dataclass

import httpx
from google.transit import gtfs_realtime_pb2

from stations import MTA_STATIONS

# Feed URLs grouped by lines they serve
FEED_URLS: dict[str, str] = {
    "ACE": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "BDFM": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "JZ": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "NQRW": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "L": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "1234567": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "SIR": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
}

ALERTS_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json"


@dataclass
class Arrival:
    line: str
    direction: str  # "N" or "S"
    terminal: str  # e.g., "Inwood-207 St"
    minutes: int


def _feed_url_for_line(line: str) -> str | None:
    """Return the GTFS feed URL that contains data for the given line."""
    for lines_key, url in FEED_URLS.items():
        if line.upper() in lines_key:
            return url
    return None


def _parse_arrivals(
    feed_data: bytes,
    station_ids: set[str],
    lines: set[str],
    now: float,
) -> list[Arrival]:
    """Parse GTFS-realtime feed and extract arrivals for requested stations/lines."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_data)

    arrivals: list[Arrival] = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip = entity.trip_update
        route_id = trip.trip.route_id

        if route_id not in lines:
            continue

        for stop_update in trip.stop_time_update:
            stop_id = stop_update.stop_id
            
            # Match exact stop_id or base station (e.g., "L06" matches "L06N" and "L06S")
            matches = (
                stop_id in station_ids
                or (len(stop_id) > 1 and stop_id[:-1] in station_ids)
            )
            
            if not matches:
                continue

            arrival_time = stop_update.arrival.time
            minutes = (arrival_time - now) / 60

            # Skip arrivals in the past or too far in the future
            if minutes < 0 or minutes > 200:
                continue

            # Get terminal station name
            terminal_stop_id = trip.stop_time_update[-1].stop_id
            terminal_name = MTA_STATIONS.get(terminal_stop_id, "Unknown")

            # Extract direction from stop_id suffix (N/S)
            # MTA stop_ids end with N or S (e.g., "L06N", "L06S")
            direction = stop_id[-1] if stop_id[-1] in ("N", "S") else "N"
            
            arrivals.append(
                Arrival(
                    line=route_id,
                    direction=direction,
                    terminal=terminal_name,
                    minutes=0 if minutes < 1 else round(minutes),
                )
            )

    return arrivals


async def get_arrivals(
    station_ids: set[str],
    lines: set[str],
    client: httpx.AsyncClient,
) -> dict:
    """
    Fetch MTA subway arrivals for the given stations and lines.

    Args:
        station_ids: Set of station IDs (e.g., {"A32N", "A32S"})
        lines: Set of subway lines (e.g., {"A", "C", "E"})
        client: httpx async client for making requests

    Returns:
        Dict with "North" and "South" keys, each containing sorted arrival lists.
        Format matches legacy API for firmware backward compatibility.
    """
    now = time.time()

    # Determine which feeds we need to fetch
    urls_to_fetch = set()
    for line in lines:
        url = _feed_url_for_line(line)
        if url:
            urls_to_fetch.add(url)

    # Fetch all relevant feeds concurrently
    responses = await asyncio.gather(
        *[client.get(url) for url in urls_to_fetch],
        return_exceptions=True,
    )

    # Parse arrivals from all feeds
    all_arrivals: list[Arrival] = []
    for response in responses:
        if isinstance(response, Exception):
            continue
        all_arrivals.extend(
            _parse_arrivals(response.content, station_ids, lines, now)
        )

    # Split by direction and sort by arrival time
    north = sorted(
        [a for a in all_arrivals if a.direction == "N"],
        key=lambda a: a.minutes,
    )[:11]

    south = sorted(
        [a for a in all_arrivals if a.direction == "S"],
        key=lambda a: a.minutes,
    )[:11]

    # Format for backward compatibility with firmware
    return {
        "North": [
            {
                "Line": a.line,
                "N-S": a.direction,
                "Direction": a.terminal,
                "Arrival": a.minutes,
            }
            for a in north
        ],
        "South": [
            {
                "Line": a.line,
                "N-S": a.direction,
                "Direction": a.terminal,
                "Arrival": a.minutes,
            }
            for a in south
        ],
    }


async def get_alerts(lines: set[str], client: httpx.AsyncClient) -> list[str]:
    """
    Fetch active service alerts for the given subway lines.

    Args:
        lines: Set of subway lines (e.g., {"A", "C", "E"})
        client: httpx async client for making requests

    Returns:
        List of alert text strings for affected lines.
    """
    try:
        response = await client.get(ALERTS_URL, timeout=5.0)
        response.raise_for_status()
        data = response.json()
    except (httpx.RequestError, httpx.HTTPStatusError):
        return []

    now = int(time.time())
    active_alerts: set[str] = set()

    for alert in data.get("entity", []):
        try:
            alert_info = alert["alert"]
            route_id = alert_info["informed_entity"][0]["route_id"]

            if route_id not in lines:
                continue

            for period in alert_info["active_period"]:
                if period["start"] < now < period["end"]:
                    text = alert_info["header_text"]["translation"][0]["text"]
                    active_alerts.add(text.replace("\n", " ").strip())
                    break
        except (KeyError, IndexError):
            continue

    return list(active_alerts)
