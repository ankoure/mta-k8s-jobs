import json
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal

import boto3

from config import AWS_REGION, S3_BUCKET_NAME, S3_ENDPOINT_URL, START_DATE
from dashboard import build_dashboard_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal types from DynamoDB in JSON serialization."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def upload_to_s3(json_str: str, key: str) -> None:
    """Upload a JSON string to S3."""
    client_kwargs = {"region_name": AWS_REGION}
    if S3_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = S3_ENDPOINT_URL
    s3 = boto3.client("s3", **client_kwargs)
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=json_str.encode("utf-8"),
        ContentType="application/json",
    )
    log.info("Uploaded s3://%s/%s", S3_BUCKET_NAME, key)


def main() -> None:
    today = datetime.now(tz=timezone.utc).date()
    log.info("service-ridership-dashboard starting (date=%s)", today.isoformat())

    try:
        dash_json = build_dashboard_json(START_DATE, today)
    except Exception as exc:
        log.error("Failed to build dashboard JSON: %s", exc, exc_info=True)
        sys.exit(1)

    json_str = json.dumps(dash_json, cls=DecimalEncoder)
    log.info("Dashboard JSON size: %.1f KB", len(json_str) / 1024)

    try:
        upload_to_s3(json_str, f"{today.isoformat()}.json")
        upload_to_s3(json_str, "latest.json")
    except Exception as exc:
        log.error("Failed to upload to S3: %s", exc, exc_info=True)
        sys.exit(1)

    log.info("service-ridership-dashboard complete")


if __name__ == "__main__":
    main()
