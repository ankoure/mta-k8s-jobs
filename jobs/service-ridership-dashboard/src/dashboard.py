import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal, Optional, TypedDict

from config import (
    DASHBOARD_LINE_IDS,
    LINE_KIND_TO_MODE,
    LINE_METADATA,
    PRE_COVID_DATE,
)
from queries import query_ridership
from service_levels import (
    ServiceLevelsByDate,
    ServiceLevelsEntry,
    get_service_levels_by_line,
)
from time_series import (
    WeeklyMedianTimeSeries,
    get_latest_weekly_median_time_series_entry,
    get_weekly_median_time_series,
    merge_weekly_median_time_series,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------

LineKind = Literal["rapid-transit", "bus", "regional-rail"]
ModeKind = Literal["rapid-transit", "bus", "regional-rail"]


class ServiceSummaryForDay(TypedDict):
    cancelled: bool
    tripsPerHour: Optional[list[int]]
    totalTrips: int


class ServiceSummary(TypedDict):
    weekday: ServiceSummaryForDay
    saturday: ServiceSummaryForDay
    sunday: ServiceSummaryForDay


class ServiceRegimes(TypedDict):
    current: ServiceSummary
    oneYearAgo: ServiceSummary
    baseline: ServiceSummary


class LineData(TypedDict):
    id: str
    shortName: str
    longName: str
    lineKind: LineKind
    startDate: str
    ridershipHistory: WeeklyMedianTimeSeries
    serviceHistory: WeeklyMedianTimeSeries
    serviceRegimes: ServiceRegimes


class SummaryData(TypedDict):
    totalRidershipHistory: WeeklyMedianTimeSeries
    totalServiceHistory: WeeklyMedianTimeSeries
    totalPassengers: float
    totalTrips: float
    totalRoutesCancelled: int
    totalReducedService: int
    totalIncreasedService: int
    startDate: str
    endDate: str


class DashJSON(TypedDict):
    lineData: dict[str, LineData]
    summaryData: SummaryData
    modeData: dict[str, SummaryData]


# ---------------------------------------------------------------------------
# Ridership helpers
# ---------------------------------------------------------------------------


@dataclass
class RidershipEntry:
    date: date
    ridership: float


def _load_ridership_by_line(
    start_date: date, end_date: date, line_ids: list[str]
) -> dict[str, dict[date, RidershipEntry]]:
    """Query ridership for each line and organize by date."""
    result: dict[str, dict[date, RidershipEntry]] = {}
    for line_id in line_ids:
        raw_items = query_ridership(line_id, start_date, end_date)
        entries: dict[date, RidershipEntry] = {}
        for item in raw_items:
            d = date.fromisoformat(item["date"].split(" ")[0])
            entries[d] = RidershipEntry(date=d, ridership=float(item["count"]))
        result[line_id] = entries
    return result


# ---------------------------------------------------------------------------
# Service summaries (ported from MBTA service_summaries.py)
# ---------------------------------------------------------------------------


def _is_matching_entry(
    entry: ServiceLevelsEntry,
    valid_range: tuple[date, date],
    matching_days: list[int],
    require_typical: bool,
) -> bool:
    start, end = valid_range
    return (
        start <= entry.date <= end
        and entry.date.weekday() in matching_days
        and (not require_typical or not entry.has_service_exceptions)
    )


def _get_matching_entry(
    service_levels: ServiceLevelsByDate,
    start_lookback: date,
    max_lookback_days: int,
    matching_days: list[int],
    require_typical: bool,
) -> Optional[ServiceLevelsEntry]:
    """Find the most recent service entry matching day-of-week criteria."""
    end_lookback = start_lookback - timedelta(days=max_lookback_days)
    for lookback_date in sorted(service_levels.keys(), reverse=True):
        if _is_matching_entry(
            service_levels[lookback_date],
            (end_lookback, start_lookback),
            matching_days,
            require_typical,
        ):
            return service_levels[lookback_date]
    return None


def _is_service_cancelled(
    service_levels: ServiceLevelsByDate,
    start_lookback: date,
    matching_days: list[int],
) -> bool:
    return (
        _get_matching_entry(
            service_levels,
            start_lookback,
            max_lookback_days=7,
            matching_days=matching_days,
            require_typical=False,
        )
        is None
    )


def _get_summary_for_day(
    start_lookback: date,
    service_levels: ServiceLevelsByDate,
    matching_days: list[int],
) -> ServiceSummaryForDay:
    if _is_service_cancelled(service_levels, start_lookback, matching_days):
        return {"cancelled": True, "tripsPerHour": None, "totalTrips": 0}

    entry = _get_matching_entry(
        service_levels,
        start_lookback,
        max_lookback_days=1000 * 365,
        matching_days=matching_days,
        require_typical=True,
    )
    if entry is None:
        return {"cancelled": True, "tripsPerHour": None, "totalTrips": 0}

    return {
        "cancelled": False,
        "tripsPerHour": entry.service_levels,
        "totalTrips": round(sum(entry.service_levels)),
    }


def _summarize_weekly_service(
    d: date, service_levels: ServiceLevelsByDate
) -> ServiceSummary:
    return {
        "weekday": _get_summary_for_day(d, service_levels, list(range(5))),
        "saturday": _get_summary_for_day(d, service_levels, [5]),
        "sunday": _get_summary_for_day(d, service_levels, [6]),
    }


def _create_service_regimes(
    service_levels: ServiceLevelsByDate, d: date
) -> ServiceRegimes:
    return {
        "current": _summarize_weekly_service(d, service_levels),
        "oneYearAgo": _summarize_weekly_service(
            d - timedelta(days=365), service_levels
        ),
        "baseline": _summarize_weekly_service(PRE_COVID_DATE, service_levels),
    }


# ---------------------------------------------------------------------------
# Line data assembly
# ---------------------------------------------------------------------------


def _create_line_data(
    line_id: str,
    start_date: date,
    end_date: date,
    service_levels: ServiceLevelsByDate,
    ridership: dict[date, RidershipEntry],
) -> LineData:
    meta = LINE_METADATA[line_id]
    latest_date = max(service_levels.keys())

    return {
        "id": line_id,
        "shortName": meta["shortName"],
        "longName": meta["longName"],
        "lineKind": meta["lineKind"],
        "startDate": start_date.isoformat(),
        "ridershipHistory": get_weekly_median_time_series(
            entries=ridership,
            entry_value_getter=lambda e: e.ridership,
            start_date=start_date,
            max_end_date=end_date,
        ),
        "serviceHistory": get_weekly_median_time_series(
            entries=service_levels,
            entry_value_getter=lambda e: round(sum(e.service_levels)),
            start_date=start_date,
            max_end_date=end_date,
        ),
        "serviceRegimes": _create_service_regimes(service_levels, latest_date),
    }


# ---------------------------------------------------------------------------
# Summary / mode aggregation (ported from MBTA summary.py)
# ---------------------------------------------------------------------------


def _line_is_cancelled(line: LineData) -> bool:
    return line["serviceRegimes"]["current"]["weekday"]["cancelled"]


def _line_has_reduced_service(line: LineData) -> bool:
    try:
        last_year = line["serviceRegimes"]["oneYearAgo"]["weekday"]["totalTrips"]
        current = line["serviceRegimes"]["current"]["weekday"]["totalTrips"]
        return current / last_year < (19 / 20)
    except ZeroDivisionError:
        return False


def _line_has_increased_service(line: LineData) -> bool:
    try:
        last_year = line["serviceRegimes"]["oneYearAgo"]["weekday"]["totalTrips"]
        current = line["serviceRegimes"]["current"]["weekday"]["totalTrips"]
        return current / last_year > (20 / 19)
    except ZeroDivisionError:
        return False


def _get_summary_data(
    line_data: list[LineData], start_date: date, end_date: date
) -> SummaryData:
    total_ridership = merge_weekly_median_time_series(
        [ld["ridershipHistory"] for ld in line_data]
    )
    total_service = merge_weekly_median_time_series(
        [ld["serviceHistory"] for ld in line_data]
    )
    return {
        "totalRidershipHistory": total_ridership,
        "totalServiceHistory": total_service,
        "totalPassengers": get_latest_weekly_median_time_series_entry(total_ridership)
        or 0,
        "totalTrips": get_latest_weekly_median_time_series_entry(total_service) or 0,
        "totalRoutesCancelled": sum(_line_is_cancelled(ld) for ld in line_data),
        "totalReducedService": sum(_line_has_reduced_service(ld) for ld in line_data),
        "totalIncreasedService": sum(
            _line_has_increased_service(ld) for ld in line_data
        ),
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
    }


def _get_summary_by_mode(
    line_data: list[LineData], start_date: date, end_date: date
) -> dict[str, SummaryData]:
    lines_by_mode: dict[str, list[LineData]] = defaultdict(list)
    for ld in line_data:
        mode = LINE_KIND_TO_MODE.get(ld["lineKind"], ld["lineKind"])
        lines_by_mode[mode].append(ld)
    return {
        mode: _get_summary_data(lines, start_date, end_date)
        for mode, lines in lines_by_mode.items()
    }


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


def build_dashboard_json(start_date: date, end_date: date) -> DashJSON:
    """Build the complete dashboard JSON structure."""
    log.info("Loading service levels...")
    service_by_line = get_service_levels_by_line(start_date, end_date)

    # Only include lines that have service data
    active_line_ids = [
        lid
        for lid in DASHBOARD_LINE_IDS
        if lid in service_by_line and service_by_line[lid]
    ]
    log.info("Active lines with service data: %s", active_line_ids)

    log.info("Loading ridership...")
    ridership_by_line = _load_ridership_by_line(start_date, end_date, active_line_ids)

    # Build per-line data (only for lines with both service and ridership)
    line_data_by_id: dict[str, LineData] = {}
    for line_id in active_line_ids:
        service = service_by_line.get(line_id, {})
        ridership = ridership_by_line.get(line_id, {})
        if not service or not ridership:
            log.warning("Skipping %s: missing service or ridership data", line_id)
            continue
        line_data_by_id[line_id] = _create_line_data(
            line_id, start_date, end_date, service, ridership
        )
        log.info("Built line data for %s", line_id)

    all_lines = list(line_data_by_id.values())
    summary = _get_summary_data(all_lines, start_date, end_date)
    mode_data = _get_summary_by_mode(all_lines, start_date, end_date)

    return {
        "lineData": line_data_by_id,
        "summaryData": summary,
        "modeData": mode_data,
    }
