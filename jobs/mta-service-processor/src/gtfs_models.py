from typing import List, Dict
from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass
class GtfsCalendarService:
    service_id: str
    monday: int
    tuesday: int
    wednesday: int
    thursday: int
    friday: int
    saturday: int
    sunday: int
    start_date: date
    end_date: date


@dataclass
class GtfsCalendarException:
    service_id: str
    date: date
    exception_type: int  # 1 = added, 2 = removed


EXCEPTION_TYPE_ADDED = 1
EXCEPTION_TYPE_REMOVED = 2


@dataclass
class GtfsTrip:
    trip_id: str
    route_id: str
    service_id: str
    direction_id: str
    start_time: int = 0  # seconds since midnight, populated from stop_times
    end_time: int = 0  # seconds since midnight, populated from stop_times


@dataclass
class GtfsRoute:
    route_id: str
    agency_id: str
    route_short_name: str
    route_long_name: str
    route_type: int


@dataclass
class GtfsFeedData:
    calendar_services: Dict[str, GtfsCalendarService] = field(default_factory=dict)
    calendar_exceptions: Dict[str, List[GtfsCalendarException]] = field(
        default_factory=dict
    )
    trips_by_route_id: Dict[str, List[GtfsTrip]] = field(default_factory=dict)
    routes: Dict[str, GtfsRoute] = field(default_factory=dict)


@dataclass
class RouteDateTotals:
    agency_id: str
    route_id: str
    route_short_name: str
    route_long_name: str
    date: date
    count: int
    service_minutes: int
    by_hour: List[int]
    has_service_exceptions: bool

    @property
    def timestamp(self) -> float:
        dt = datetime.combine(self.date, datetime.min.time())
        return dt.timestamp()
