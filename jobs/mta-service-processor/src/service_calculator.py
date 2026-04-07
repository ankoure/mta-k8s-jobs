from datetime import date, timedelta
from typing import Callable, Dict, Generator, List

from gtfs_models import (
    EXCEPTION_TYPE_ADDED,
    EXCEPTION_TYPE_REMOVED,
    GtfsFeedData,
    GtfsRoute,
    GtfsTrip,
    RouteDateTotals,
)


def bucket_trips_by_hour(trips: List[GtfsTrip]) -> List[int]:
    """Count trips by their starting hour of the day."""
    by_time_of_day = [0] * 24
    for trip in trips:
        hour = (trip.start_time // 3600) % 24
        by_time_of_day[hour] += 1
    return by_time_of_day


def get_total_service_minutes(trips: List[GtfsTrip]) -> int:
    """Calculate the total service time in minutes across all trips."""
    return sum(trip.end_time - trip.start_time for trip in trips) // 60


def date_range(start_date: date, end_date: date) -> Generator[date, None, None]:
    """Yield each date from start_date to end_date inclusive."""
    assert start_date <= end_date
    now = start_date
    while now <= end_date:
        yield now
        now = now + timedelta(days=1)


def get_service_ids_for_date(feed_data: GtfsFeedData, today: date) -> Dict[str, bool]:
    """Return active service IDs for a date, mapped to whether exceptions apply.

    Handles two cases:
    - Feeds with calendar.txt: checks date range + day-of-week, then exceptions
    - Feeds with only calendar_dates.txt: services are active only via ADDED exceptions
    """
    services_for_today: Dict[str, bool] = {}

    # Process services from calendar.txt
    for service_id, service in feed_data.calendar_services.items():
        service_exceptions = feed_data.calendar_exceptions.get(service_id, [])
        in_range = service.start_date <= today <= service.end_date
        on_service_day = [
            service.monday,
            service.tuesday,
            service.wednesday,
            service.thursday,
            service.friday,
            service.saturday,
            service.sunday,
        ][today.weekday()] == 1
        exceptions_today = [ex for ex in service_exceptions if ex.date == today]
        is_removed = any(ex.exception_type == EXCEPTION_TYPE_REMOVED for ex in exceptions_today)
        is_added = any(ex.exception_type == EXCEPTION_TYPE_ADDED for ex in exceptions_today)
        if is_added or (in_range and on_service_day and not is_removed):
            services_for_today[service_id] = len(exceptions_today) > 0

    # Handle calendar_dates-only services (service_ids not in calendar.txt)
    if not feed_data.calendar_services:
        for service_id, exceptions in feed_data.calendar_exceptions.items():
            exceptions_today = [ex for ex in exceptions if ex.date == today]
            is_added = any(ex.exception_type == EXCEPTION_TYPE_ADDED for ex in exceptions_today)
            if is_added:
                services_for_today[service_id] = True

    return services_for_today


def create_route_date_totals(
    today: date,
    feed_data: GtfsFeedData,
    agency_id: str,
    route_filter: Callable[[GtfsRoute], bool] | None = None,
) -> List[RouteDateTotals]:
    """Create scheduled service totals for all routes on a given date."""
    all_totals = []
    active_services = get_service_ids_for_date(feed_data, today)

    for route_id, route in feed_data.routes.items():
        if route_filter and not route_filter(route):
            continue

        trips = [
            trip
            for trip in feed_data.trips_by_route_id.get(route_id, [])
            if trip.service_id in active_services
        ]

        if not trips:
            continue

        has_service_exceptions = any(
            active_services.get(trip.service_id, False) for trip in trips
        )

        totals = RouteDateTotals(
            agency_id=agency_id,
            route_id=route_id,
            route_short_name=route.route_short_name,
            route_long_name=route.route_long_name,
            date=today,
            count=len(trips),
            by_hour=bucket_trips_by_hour(trips),
            has_service_exceptions=has_service_exceptions,
            service_minutes=get_total_service_minutes(trips),
        )
        all_totals.append(totals)

    return all_totals
