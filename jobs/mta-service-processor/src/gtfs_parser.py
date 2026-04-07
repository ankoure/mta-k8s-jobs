import csv
import io
import logging
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import requests

from gtfs_models import (
    GtfsCalendarException,
    GtfsCalendarService,
    GtfsFeedData,
    GtfsRoute,
    GtfsTrip,
)

logger = logging.getLogger(__name__)


def parse_gtfs_time(time_str: str) -> int:
    """Parse a GTFS time string (HH:MM:SS) to seconds since midnight.

    GTFS times can exceed 24:00:00 for trips spanning past midnight.
    """
    parts = time_str.strip().split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])


def parse_gtfs_date(date_str: str) -> date:
    """Parse a GTFS date string (YYYYMMDD) to a date object."""
    return datetime.strptime(date_str.strip(), "%Y%m%d").date()


def download_gtfs_zip(url: str, dest_path: Path) -> Path:
    """Download a GTFS zip file from a URL."""
    logger.info(f"Downloading GTFS feed from {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    dest_path.write_bytes(response.content)
    logger.info(f"Downloaded {len(response.content) / 1024 / 1024:.1f} MB to {dest_path}")
    return dest_path


def _read_csv_from_zip(zip_path: Path, filename: str) -> csv.DictReader | None:
    """Read a CSV file from within a zip archive. Returns None if file doesn't exist."""
    zf = zipfile.ZipFile(zip_path, "r")
    try:
        raw = zf.open(filename)
    except KeyError:
        zf.close()
        return None
    text = io.TextIOWrapper(raw, encoding="utf-8-sig")
    return csv.DictReader(text)


def parse_calendar(zip_path: Path) -> List[GtfsCalendarService]:
    """Parse calendar.txt from a GTFS zip. Returns empty list if file is absent."""
    reader = _read_csv_from_zip(zip_path, "calendar.txt")
    if reader is None:
        logger.info("No calendar.txt found in feed, using calendar_dates.txt only")
        return []
    services = []
    for row in reader:
        services.append(
            GtfsCalendarService(
                service_id=row["service_id"],
                monday=int(row["monday"]),
                tuesday=int(row["tuesday"]),
                wednesday=int(row["wednesday"]),
                thursday=int(row["thursday"]),
                friday=int(row["friday"]),
                saturday=int(row["saturday"]),
                sunday=int(row["sunday"]),
                start_date=parse_gtfs_date(row["start_date"]),
                end_date=parse_gtfs_date(row["end_date"]),
            )
        )
    reader._fieldnames = None  # allow GC of underlying zip handles
    return services


def parse_calendar_dates(zip_path: Path) -> List[GtfsCalendarException]:
    """Parse calendar_dates.txt from a GTFS zip. Returns empty list if file is absent."""
    reader = _read_csv_from_zip(zip_path, "calendar_dates.txt")
    if reader is None:
        return []
    exceptions = []
    for row in reader:
        exceptions.append(
            GtfsCalendarException(
                service_id=row["service_id"],
                date=parse_gtfs_date(row["date"]),
                exception_type=int(row["exception_type"]),
            )
        )
    return exceptions


def parse_routes(zip_path: Path) -> List[GtfsRoute]:
    """Parse routes.txt from a GTFS zip."""
    reader = _read_csv_from_zip(zip_path, "routes.txt")
    if reader is None:
        raise ValueError("routes.txt is required in a GTFS feed")
    routes = []
    for row in reader:
        routes.append(
            GtfsRoute(
                route_id=row["route_id"],
                agency_id=row.get("agency_id", ""),
                route_short_name=row.get("route_short_name", ""),
                route_long_name=row.get("route_long_name", ""),
                route_type=int(row.get("route_type", 0)),
            )
        )
    return routes


def parse_trips(zip_path: Path) -> List[GtfsTrip]:
    """Parse trips.txt from a GTFS zip. Trips will not have start/end times yet."""
    reader = _read_csv_from_zip(zip_path, "trips.txt")
    if reader is None:
        raise ValueError("trips.txt is required in a GTFS feed")
    trips = []
    for row in reader:
        trips.append(
            GtfsTrip(
                trip_id=row["trip_id"],
                route_id=row["route_id"],
                service_id=row["service_id"],
                direction_id=row.get("direction_id", "0"),
            )
        )
    return trips


def parse_stop_time_bounds(zip_path: Path) -> Dict[str, Tuple[int, int]]:
    """Stream stop_times.txt to extract min arrival and max departure per trip.

    This is memory-efficient: only keeps a dict of {trip_id: (min_time, max_time)},
    never loading the full file into memory.
    """
    reader = _read_csv_from_zip(zip_path, "stop_times.txt")
    if reader is None:
        raise ValueError("stop_times.txt is required in a GTFS feed")
    bounds: Dict[str, Tuple[int, int]] = {}
    count = 0
    for row in reader:
        trip_id = row["trip_id"]
        arrival = row.get("arrival_time", "").strip()
        departure = row.get("departure_time", "").strip()
        if not arrival and not departure:
            continue
        arrival_secs = parse_gtfs_time(arrival) if arrival else None
        departure_secs = parse_gtfs_time(departure) if departure else None
        time_min = arrival_secs if arrival_secs is not None else departure_secs
        time_max = departure_secs if departure_secs is not None else arrival_secs
        if trip_id in bounds:
            existing_min, existing_max = bounds[trip_id]
            bounds[trip_id] = (min(existing_min, time_min), max(existing_max, time_max))
        else:
            bounds[trip_id] = (time_min, time_max)
        count += 1
        if count % 1_000_000 == 0:
            logger.info(f"  Processed {count:,} stop_time rows ({len(bounds):,} trips)")
    logger.info(f"  Processed {count:,} stop_time rows total ({len(bounds):,} trips)")
    return bounds


def _bucket_by(items, key_getter):
    """Group items into lists by a key."""
    res = {}
    for item in items:
        key = key_getter(item)
        res.setdefault(key, [])
        res[key].append(item)
    return res


def _index_by(items, key_getter):
    """Index items into a dict by a key."""
    return {key_getter(item): item for item in items}


def load_feed(url: str, dest_dir: Path) -> GtfsFeedData:
    """Download and parse a GTFS feed into an indexed GtfsFeedData container."""
    zip_path = dest_dir / "gtfs.zip"
    download_gtfs_zip(url, zip_path)

    logger.info("Parsing calendar.txt")
    calendar_services = parse_calendar(zip_path)
    logger.info(f"  Found {len(calendar_services)} calendar services")

    logger.info("Parsing calendar_dates.txt")
    calendar_exceptions = parse_calendar_dates(zip_path)
    logger.info(f"  Found {len(calendar_exceptions)} calendar date exceptions")

    logger.info("Parsing routes.txt")
    routes = parse_routes(zip_path)
    logger.info(f"  Found {len(routes)} routes")

    logger.info("Parsing trips.txt")
    trips = parse_trips(zip_path)
    logger.info(f"  Found {len(trips)} trips")

    logger.info("Parsing stop_times.txt (streaming)")
    stop_time_bounds = parse_stop_time_bounds(zip_path)

    # Attach start/end times to trips
    trips_with_times = []
    skipped = 0
    for trip in trips:
        if trip.trip_id in stop_time_bounds:
            trip.start_time, trip.end_time = stop_time_bounds[trip.trip_id]
            trips_with_times.append(trip)
        else:
            skipped += 1
    if skipped:
        logger.warning(f"  Skipped {skipped} trips with no stop_times data")

    # Free memory from stop_time_bounds
    del stop_time_bounds

    return GtfsFeedData(
        calendar_services=_index_by(calendar_services, lambda s: s.service_id),
        calendar_exceptions=_bucket_by(calendar_exceptions, lambda e: e.service_id),
        trips_by_route_id=_bucket_by(trips_with_times, lambda t: t.route_id),
        routes=_index_by(routes, lambda r: r.route_id),
    )
