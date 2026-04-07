"""
ridership-processor/src/main.py

Fetches MTA daily ridership CSV data, converts it to Parquet with DuckDB,
and writes rows to a DynamoDB table.

Runs every hour as a Kubernetes CronJob.
"""

import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3
import duckdb
import requests

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
# Configuration (all values come from environment variables)
# ---------------------------------------------------------------------------

# MTA Daily Ridership and Traffic (Beginning 2020).
# Columns: date (MM/DD/YYYY), mode (text), count (number).
# Override DATA_URL to point at a different endpoint or a local test file.
DATA_URL = os.environ.get(
    "DATA_URL",
    "https://data.ny.gov/api/views/sayj-mze2/rows.csv?accessType=DOWNLOAD",
)

# AWS DynamoDB configuration.
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME", "Ridership")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

TMP_DIR = Path("/tmp/ridership")

# ---------------------------------------------------------------------------
# Line ID mapping — normalise raw MTA mode names to "line-{Name}" format
# ---------------------------------------------------------------------------

LINE_ID_MAP: dict[str, str] = {
    "Subway":      "line-subway",
    "SIR":         "line-sir",
    "MNR":         "line-mnr",
    "LIRR":        "line-lirr",
    "Bus":         "line-bus",
    "BT":          "line-bridgesandtunnels",
    "CRZ Entries": "line-crzentries",
    "CBD Entries":  "line-cbdentries",
}


def normalize_line_id(raw_mode: str) -> str:
    """Map a raw MTA mode string to a normalised line ID.

    Returns the mapped value if found, otherwise falls back to
    ``line-{raw_mode}`` so new modes are still ingested rather than dropped.
    """
    mapped = LINE_ID_MAP.get(raw_mode)
    if mapped:
        return mapped
    # Fallback: strip whitespace, replace spaces with hyphens
    cleaned = raw_mode.strip().replace(" ", "-")
    log.warning("Unmapped mode %r — falling back to line-%s", raw_mode, cleaned)
    return f"line-{cleaned}"

# ---------------------------------------------------------------------------
# Step 1: Fetch
# ---------------------------------------------------------------------------


def fetch_data() -> Path:
    """Download the raw ridership CSV and save it to /tmp."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    dest = TMP_DIR / "raw.csv"

    log.info("Fetching ridership data from %s", DATA_URL)
    response = requests.get(DATA_URL, timeout=120, stream=True)
    response.raise_for_status()

    with dest.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = dest.stat().st_size / 1_048_576
    log.info("Downloaded %.2f MB to %s", size_mb, dest)
    return dest


# ---------------------------------------------------------------------------
# Step 2: Process — convert CSV to Parquet
# ---------------------------------------------------------------------------


def process_data(raw_csv: Path) -> Path:
    """
    Use DuckDB to convert the daily ridership CSV to a Parquet file.

    The source CSV has columns: date, mode, count.
    The output Parquet normalises these to: date (DATE), lineId (VARCHAR),
    ridership (BIGINT) — matching the DynamoDB table schema.
    """
    parquet_out = TMP_DIR / "daily_ridership.parquet"

    log.info("Processing data with DuckDB")
    con = duckdb.connect()

    con.execute(f"""
        CREATE TABLE raw AS
        SELECT * FROM read_csv_auto('{raw_csv}', header=true)
    """)

    row_count = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    log.info("Loaded raw CSV; row count: %d", row_count)

    con.execute(f"""
        COPY (
            SELECT
                CAST("Date" AS DATE)   AS date,
                "Mode"                 AS "lineId",
                CAST("Count" AS BIGINT) AS ridership
            FROM raw
            ORDER BY date, "lineId"
        ) TO '{parquet_out}' (FORMAT PARQUET)
    """)
    log.info("Wrote %s", parquet_out)

    con.close()
    return parquet_out


# ---------------------------------------------------------------------------
# Step 3: Write to DynamoDB
# ---------------------------------------------------------------------------


def write_to_dynamodb(parquet_file: Path) -> None:
    """Write daily ridership rows to the Ridership DynamoDB table.

    Each row becomes an item with:
      lineId    (S) — partition key, the MTA mode (e.g. Subway, Bus, LIRR)
      date      (S) — sort key, ISO date string (YYYY-MM-DD)
      ridership (N) — estimated daily ridership / traffic count
    """
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT date, "lineId", ridership
        FROM read_parquet('{parquet_file}')
    """).fetchall()
    con.close()

    log.info("Writing %d items to DynamoDB table %s", len(rows), DYNAMODB_TABLE_NAME)

    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    with table.batch_writer() as batch:
        for date, raw_mode, ridership in rows:
            batch.put_item(Item={
                "lineId": normalize_line_id(str(raw_mode)),
                "date": str(date),
                "ridership": Decimal(str(ridership)),
            })

    log.info("DynamoDB write complete")


# ---------------------------------------------------------------------------
# Step 4: Cleanup
# ---------------------------------------------------------------------------


def cleanup() -> None:
    """Remove temporary files written during this run."""
    log.info("Cleaning up %s", TMP_DIR)
    for f in TMP_DIR.iterdir():
        f.unlink()
    TMP_DIR.rmdir()
    log.info("Cleanup complete")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    run_ts = datetime.now(tz=timezone.utc)
    log.info("ridership-processor starting (run_ts=%s)", run_ts.isoformat())

    try:
        raw_csv = fetch_data()
    except Exception as exc:
        log.error("Fetch failed: %s", exc)
        sys.exit(1)

    try:
        parquet_file = process_data(raw_csv)
    except Exception as exc:
        log.error("Processing failed: %s", exc)
        sys.exit(1)

    try:
        write_to_dynamodb(parquet_file)
    except Exception as exc:
        log.error("DynamoDB write failed: %s", exc)
        sys.exit(1)

    try:
        cleanup()
    except Exception as exc:
        log.warning("Cleanup failed (non-fatal): %s", exc)

    log.info("ridership-processor finished successfully")


if __name__ == "__main__":
    main()
