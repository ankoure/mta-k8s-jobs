"""
mta-service-processor/src/main.py

Downloads MTA GTFS static feeds, computes scheduled service totals
(trip counts, service minutes, hourly distribution) per route per date,
and writes results to DynamoDB.

Supports two modes:
  - Daily (default): process the latest GTFS feed for each agency
  - Backfill (--backfill): process historical GTFS snapshots from MobilityDatabase
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3

from config import DYNAMODB_TABLE_NAME, MOBILITYDB_BASE_URL, MTA_FEEDS, AgencyFeedConfig
from gtfs_models import RouteDateTotals
from gtfs_parser import load_feed
from mobility_api import MobilityApiClient, MobilityApiError
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
# MobilityDatabase API
# ---------------------------------------------------------------------------


def create_api_client() -> MobilityApiClient | None:
    """Create a MobilityDB client if the refresh token env var is set."""
    token = os.environ.get("MOBILITYDB_REFRESH_TOKEN")
    if not token:
        log.warning("MOBILITYDB_REFRESH_TOKEN not set, using direct MTA URLs")
        return None
    return MobilityApiClient(refresh_token=token, base_url=MOBILITYDB_BASE_URL)


# ---------------------------------------------------------------------------
# Step 1: Fetch and parse a GTFS feed
# ---------------------------------------------------------------------------


def fetch_and_parse(url: str, agency_id: str, tmp_dir: Path):
    """Download and parse a single GTFS feed from a URL."""
    feed_dir = tmp_dir / agency_id
    feed_dir.mkdir(parents=True, exist_ok=True)
    return load_feed(url, feed_dir)


def resolve_feed_url(
    feed_config: AgencyFeedConfig, api_client: MobilityApiClient | None
) -> str:
    """Get the best available feed URL: MobilityDB latest dataset, or MTA direct."""
    if api_client and feed_config.mobilitydb_id:
        try:
            latest = api_client.get_latest_dataset(feed_config.mobilitydb_id)
            if latest:
                log.info(
                    f"Using MobilityDB dataset {latest.id} "
                    f"(downloaded {latest.downloaded_at.date()})"
                )
                return latest.download_url
        except (MobilityApiError, Exception) as exc:
            log.warning(f"MobilityDB API failed, falling back to direct URL: {exc}")
    return feed_config.feed_url


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
# DynamoDB date query (for backfill dedup)
# ---------------------------------------------------------------------------


def get_existing_dates_for_agency(agency_id: str, table_name: str) -> set[date]:
    """Scan DynamoDB for dates already computed for an agency."""
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(table_name)

    existing: set[date] = set()
    scan_kwargs = {
        "FilterExpression": boto3.dynamodb.conditions.Attr("agencyId").eq(agency_id),
        "ProjectionExpression": "#d",
        "ExpressionAttributeNames": {"#d": "date"},
    }

    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            try:
                existing.add(date.fromisoformat(item["date"]))
            except (KeyError, ValueError):
                continue
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    return existing


# ---------------------------------------------------------------------------
# Processing modes
# ---------------------------------------------------------------------------


def process_feed_daily(
    feed_config: AgencyFeedConfig, api_client: MobilityApiClient | None
) -> int:
    """Process the latest feed for an agency (normal daily mode)."""
    url = resolve_feed_url(feed_config, api_client)
    with TemporaryDirectory() as tmp_dir:
        feed_data = fetch_and_parse(url, feed_config.agency_id, Path(tmp_dir))
        totals = compute_totals(feed_config, feed_data)
        del feed_data
        write_to_dynamodb(totals, DYNAMODB_TABLE_NAME)
        return len(totals)


def process_feed_backfill(
    feed_config: AgencyFeedConfig,
    api_client: MobilityApiClient,
    after_date: date | None = None,
) -> int:
    """Process historical datasets from MobilityDB to backfill gaps."""
    if not feed_config.mobilitydb_id:
        log.warning(f"[{feed_config.agency_id}] No MobilityDB ID configured, skipping backfill")
        return 0

    after_str = after_date.isoformat() if after_date else None
    datasets = api_client.get_datasets_in_range(
        feed_config.mobilitydb_id, after=after_str
    )
    if not datasets:
        log.warning(f"[{feed_config.agency_id}] No datasets found in MobilityDB")
        return 0

    log.info(f"[{feed_config.agency_id}] Found {len(datasets)} datasets to process")

    existing_dates = get_existing_dates_for_agency(
        feed_config.agency_id, DYNAMODB_TABLE_NAME
    )
    log.info(
        f"[{feed_config.agency_id}] {len(existing_dates)} dates already in DynamoDB"
    )

    # Process oldest first so newer data overwrites older via put_item upsert
    datasets.sort(key=lambda d: d.downloaded_at)

    total_items = 0
    for i, dataset in enumerate(datasets):
        log.info(
            f"[{feed_config.agency_id}] Dataset {i + 1}/{len(datasets)}: "
            f"{dataset.id} (downloaded {dataset.downloaded_at.date()})"
        )
        try:
            with TemporaryDirectory() as tmp_dir:
                feed_data = fetch_and_parse(
                    dataset.download_url, feed_config.agency_id, Path(tmp_dir)
                )
                totals = compute_totals(feed_config, feed_data)
                del feed_data

                # Skip dates already computed (optimization for large backfills)
                if existing_dates:
                    before = len(totals)
                    totals = [t for t in totals if t.date not in existing_dates]
                    if before != len(totals):
                        log.info(
                            f"  Filtered {before - len(totals)} already-computed entries"
                        )

                write_to_dynamodb(totals, DYNAMODB_TABLE_NAME)

                for t in totals:
                    existing_dates.add(t.date)
                total_items += len(totals)

        except Exception as exc:
            log.error(f"  Dataset {dataset.id} failed: {exc}", exc_info=True)
            continue

    return total_items


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MTA GTFS service processor")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill historical data from MobilityDatabase",
    )
    parser.add_argument(
        "--after",
        type=str,
        default=None,
        help="Only backfill datasets downloaded after this date (YYYY-MM-DD)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_ts = datetime.now(tz=timezone.utc)
    log.info("mta-service-processor starting (run_ts=%s)", run_ts.isoformat())

    api_client = create_api_client()

    if args.backfill and not api_client:
        log.error("--backfill requires MOBILITYDB_REFRESH_TOKEN to be set")
        sys.exit(1)

    after_date = None
    if args.after:
        after_date = date.fromisoformat(args.after)

    feeds_to_process = MTA_FEEDS
    total_items = 0
    failed_feeds = []

    for feed_config in feeds_to_process:
        log.info(
            f"--- Processing {feed_config.display_name} ({feed_config.agency_id}) ---"
        )
        try:
            if args.backfill:
                items = process_feed_backfill(feed_config, api_client, after_date)
            else:
                items = process_feed_daily(feed_config, api_client)
            total_items += items

        except Exception as exc:
            log.error(f"[{feed_config.agency_id}] Failed: {exc}", exc_info=True)
            failed_feeds.append(feed_config.agency_id)
            continue

    mode = "backfill" if args.backfill else "daily"
    log.info(
        f"mta-service-processor finished ({mode} mode). "
        f"Wrote {total_items} total items. "
        f"Failed feeds: {failed_feeds if failed_feeds else 'none'}"
    )

    if failed_feeds:
        sys.exit(1)


if __name__ == "__main__":
    main()
