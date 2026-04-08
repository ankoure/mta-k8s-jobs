import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from config import AGENCY_TO_LINE, LINE_METADATA, SIR_ROUTE_IDS
from queries import scan_scheduled_service

log = logging.getLogger(__name__)

ServiceLevelsByDate = dict[date, "ServiceLevelsEntry"]
ServiceLevelsByLineId = dict[str, ServiceLevelsByDate]


@dataclass
class ServiceLevelsEntry:
    line_id: str
    service_levels: list[int]  # 24 hourly trip counts
    has_service_exceptions: bool
    date: date


def get_service_levels_by_line(
    start_date: date, end_date: date
) -> ServiceLevelsByLineId:
    """Load scheduled service data and aggregate from route level to line level.

    Sums hourly trip counts across all routes from all agencies that map to
    the same line, for each date.
    """
    raw_items = scan_scheduled_service(start_date, end_date)

    # Group items by (line_id, date)
    # Each bucket holds all route rows for that line on that date
    buckets: dict[str, dict[date, list[dict]]] = defaultdict(lambda: defaultdict(list))

    for item in raw_items:
        agency_id = item.get("agencyId")
        line_id = AGENCY_TO_LINE.get(agency_id)
        if line_id is None:
            continue
        # SIR routes are in the nyct_subway feed; split them into line-sir
        route_id = item.get("routeId", "")
        if agency_id == "nyct_subway" and route_id in SIR_ROUTE_IDS:
            line_id = "line-sir"
        item_date = date.fromisoformat(item["date"])
        buckets[line_id][item_date].append(item)

    # Aggregate each bucket into a ServiceLevelsEntry
    result: ServiceLevelsByLineId = {}

    for line_id, date_buckets in buckets.items():
        entries: ServiceLevelsByDate = {}
        meta = LINE_METADATA.get(line_id, {})

        for d, items in date_buckets.items():
            hourly_totals = [0] * 24
            has_exceptions = False

            for item in items:
                by_hour = item.get("byHour", {})
                totals = by_hour.get("totals", [])
                for i, count in enumerate(totals):
                    if i < 24:
                        hourly_totals[i] += int(count)
                if item.get("hasServiceExceptions", False):
                    has_exceptions = True

            entries[d] = ServiceLevelsEntry(
                line_id=line_id,
                service_levels=hourly_totals,
                has_service_exceptions=has_exceptions,
                date=d,
            )

        result[line_id] = entries
        log.info(
            "Aggregated %d service days for %s (%s)",
            len(entries),
            line_id,
            meta.get("shortName", line_id),
        )

    return result
