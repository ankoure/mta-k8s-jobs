import os
from datetime import date

# ---------------------------------------------------------------------------
# Date range
# ---------------------------------------------------------------------------

START_DATE = date(2020, 1, 1)  # Earliest available MTA ridership data
PRE_COVID_DATE = date(2020, 2, 24)  # Pre-COVID baseline (Monday)

# ---------------------------------------------------------------------------
# Agency -> Line mapping
# ---------------------------------------------------------------------------

AGENCY_TO_LINE: dict[str, str] = {
    "nyct_subway": "line-subway",
    "nyct_bus_bronx": "line-bus",
    "nyct_bus_brooklyn": "line-bus",
    "nyct_bus_manhattan": "line-bus",
    "nyct_bus_queens": "line-bus",
    "nyct_bus_staten_island": "line-bus",
    "mta_bus": "line-bus",
    "lirr": "line-lirr",
    "metro_north": "line-mnr",
}

ALL_AGENCY_IDS = list(AGENCY_TO_LINE.keys())

# SIR is included in the nyct_subway GTFS feed with routeId "SI".
# These route IDs are split out from nyct_subway into line-sir.
SIR_ROUTE_IDS = {"SI"}

DASHBOARD_LINE_IDS = ["line-subway", "line-bus", "line-lirr", "line-mnr", "line-sir"]

# ---------------------------------------------------------------------------
# Line metadata
# ---------------------------------------------------------------------------

LINE_METADATA: dict[str, dict] = {
    "line-subway": {
        "shortName": "Subway",
        "longName": "New York City Subway",
        "lineKind": "rapid-transit",
    },
    "line-bus": {
        "shortName": "Bus",
        "longName": "MTA Bus",
        "lineKind": "bus",
    },
    "line-lirr": {
        "shortName": "LIRR",
        "longName": "Long Island Rail Road",
        "lineKind": "regional-rail",
    },
    "line-mnr": {
        "shortName": "MNR",
        "longName": "Metro-North Railroad",
        "lineKind": "regional-rail",
    },
    "line-sir": {
        "shortName": "SIR",
        "longName": "Staten Island Railway",
        "lineKind": "rapid-transit",
    },
}

LINE_KIND_TO_MODE: dict[str, str] = {
    "rapid-transit": "rapid-transit",
    "bus": "bus",
    "regional-rail": "regional-rail",
}

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "mta-service-ridership-dashboard")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", None)
DYNAMODB_SERVICE_TABLE = os.environ.get(
    "DYNAMODB_SERVICE_TABLE", "ScheduledServiceDaily"
)
DYNAMODB_RIDERSHIP_TABLE = os.environ.get("DYNAMODB_RIDERSHIP_TABLE", "Ridership")
