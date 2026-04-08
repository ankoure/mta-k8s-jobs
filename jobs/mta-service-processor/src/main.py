"""
mta-service-processor/src/main.py

Downloads MTA GTFS static feeds, computes scheduled service totals
(trip counts, service minutes, hourly distribution) per route per date,
and writes results to DynamoDB.
"""

import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3

from config import DYNAMODB_TABLE_NAME, MTA_FEEDS, AgencyFeedConfig
from gtfs_models import RouteDateTotals
from gtfs_parser import load_feed
from service_calculator import create_route_date_totals, date_range

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


# ---------------------------------------------------------------------------
# Step 1: Fetch and parse a GTFS feed
# ---------------------------------------------------------------------------


def fetch_and_parse(feed_config: AgencyFeedConfig, tmp_dir: Path):
    """Download and parse a single GTFS feed."""
    feed_dir = tmp_dir / feed_config.agency_id
    feed_dir.mkdir(parents=True, exist_ok=True)
    return load_feed(feed_config.feed_url, feed_dir)


# ---------------------------------------------------------------------------
# Step 2: Compute service totals
# ---------------------------------------------------------------------------


def compute_totals(feed_config, feed_data) -> list[RouteDateTotals]:
    """Compute RouteDateTotals for all valid dates in the feed."""
    # Determine the date range from calendar services AND calendar_dates exceptions.
    # Bus feeds often publish future schedules (calendar starts days ahead)
    # but have calendar_dates exceptions covering today.
    all_dates: list[date] = []
    if feed_data.calendar_services:
        for s in feed_data.calendar_services.values():
            all_dates.append(s.start_date)
            all_dates.append(s.end_date)
    if feed_data.calendar_exceptions:
        for exceptions in feed_data.calendar_exceptions.values():
            for ex in exceptions:
                all_dates.append(ex.date)

    if not all_dates:
        log.warning(f"[{feed_config.agency_id}] No calendar data found, skipping")
        return []

    feed_start = min(all_dates)
    feed_end = min(max(all_dates), date.today())

    if feed_start > feed_end:
        log.warning(
            f"[{feed_config.agency_id}] No processable dates "
            f"(earliest={feed_start}, today={date.today()}), skipping"
        )
        return []

    log.info(
        f"[{feed_config.agency_id}] Computing service totals "
        f"from {feed_start} to {feed_end} "
        f"({(feed_end - feed_start).days + 1} days)"
    )

    all_totals = []
    for today in date_range(feed_start, feed_end):
        day_totals = create_route_date_totals(
            today=today,
            feed_data=feed_data,
            agency_id=feed_config.agency_id,
            route_filter=feed_config.route_filter,
        )
        all_totals.extend(day_totals)

    log.info(f"[{feed_config.agency_id}] Computed {len(all_totals)} route-date entries")
    return all_totals


# ---------------------------------------------------------------------------
# Step 3: Write to DynamoDB
# ---------------------------------------------------------------------------


def write_to_dynamodb(totals: list[RouteDateTotals], table_name: str) -> None:
    """Batch-write RouteDateTotals to DynamoDB."""
    if not totals:
        return

    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(table_name)

    log.info(f"Writing {len(totals)} items to DynamoDB table '{table_name}'")
    with table.batch_writer() as batch:
        for total in totals:
            item = {
                "agencyId": total.agency_id,
                "date": total.date.isoformat(),
                "timestamp": int(total.timestamp),
                "routeId": total.route_id,
                "routeShortName": total.route_short_name,
                "routeLongName": total.route_long_name,
                "count": total.count,
                "serviceMinutes": total.service_minutes,
                "hasServiceExceptions": total.has_service_exceptions,
                "byHour": {"totals": total.by_hour},
            }
            batch.put_item(Item=item)

    log.info(f"DynamoDB write complete ({len(totals)} items)")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    run_ts = datetime.now(tz=timezone.utc)
    log.info("mta-service-processor starting (run_ts=%s)", run_ts.isoformat())

    feeds_to_process = MTA_FEEDS
    total_items = 0
    failed_feeds = []

    for feed_config in feeds_to_process:
        log.info(
            f"--- Processing {feed_config.display_name} ({feed_config.agency_id}) ---"
        )
        try:
            with TemporaryDirectory() as tmp_dir:
                feed_data = fetch_and_parse(feed_config, Path(tmp_dir))
                totals = compute_totals(feed_config, feed_data)

                # Free feed_data memory before DynamoDB writes
                del feed_data

                write_to_dynamodb(totals, DYNAMODB_TABLE_NAME)
                total_items += len(totals)

        except Exception as exc:
            log.error(f"[{feed_config.agency_id}] Failed: {exc}", exc_info=True)
            failed_feeds.append(feed_config.agency_id)
            continue

    log.info(
        f"mta-service-processor finished. "
        f"Wrote {total_items} total items. "
        f"Failed feeds: {failed_feeds if failed_feeds else 'none'}"
    )

    if failed_feeds:
        sys.exit(1)


if __name__ == "__main__":
    main()
