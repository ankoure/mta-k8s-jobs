import logging
from datetime import date

import boto3
from boto3.dynamodb.conditions import Attr, Key

from config import (
    ALL_AGENCY_IDS,
    AWS_REGION,
    DYNAMODB_RIDERSHIP_TABLE,
    DYNAMODB_SERVICE_TABLE,
)

log = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)


def _paginated_query(table, **kwargs) -> list[dict]:
    """Execute a DynamoDB query with automatic pagination."""
    items = []
    response = table.query(**kwargs)
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.query(ExclusiveStartKey=response["LastEvaluatedKey"], **kwargs)
        items.extend(response["Items"])
    return items


def _paginated_scan(table, **kwargs) -> list[dict]:
    """Execute a DynamoDB scan with automatic pagination."""
    items = []
    response = table.scan(**kwargs)
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"], **kwargs)
        items.extend(response["Items"])
    return items


def scan_scheduled_service(start_date: date, end_date: date) -> list[dict]:
    """Scan ScheduledServiceDaily for all known agencies within a date range.

    Uses a full table scan with filter expressions since agencyId is not a key.
    Acceptable for a daily batch job.
    """
    table = dynamodb.Table(DYNAMODB_SERVICE_TABLE)

    filter_expr = Attr("agencyId").is_in(ALL_AGENCY_IDS) & Attr("date").between(
        start_date.isoformat(), end_date.isoformat()
    )

    items = _paginated_scan(
        table,
        FilterExpression=filter_expr,
        ProjectionExpression="routeId, #d, agencyId, byHour, hasServiceExceptions",
        ExpressionAttributeNames={"#d": "date"},
    )
    log.info("Scanned %d service items from %s", len(items), DYNAMODB_SERVICE_TABLE)
    return items


def query_ridership(line_id: str, start_date: date, end_date: date) -> list[dict]:
    """Query the Ridership table for a single line within a date range."""
    table = dynamodb.Table(DYNAMODB_RIDERSHIP_TABLE)

    items = _paginated_query(
        table,
        KeyConditionExpression=Key("lineId").eq(line_id)
        & Key("date").between(start_date.isoformat(), end_date.isoformat()),
    )
    log.info("Queried %d ridership items for %s", len(items), line_id)
    return items
