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

# Individual subway route -> line mapping.
# SIR (routeId "SI") is in the nyct_subway feed but gets its own line.
SUBWAY_ROUTE_TO_LINE: dict[str, str] = {
    "1": "line-1",
    "2": "line-2",
    "3": "line-3",
    "4": "line-4",
    "5": "line-5",
    "6": "line-6",
    "7": "line-7",
    "A": "line-A",
    "C": "line-C",
    "E": "line-E",
    "B": "line-B",
    "D": "line-D",
    "F": "line-F",
    "M": "line-M",
    "G": "line-G",
    "J": "line-J",
    "Z": "line-Z",
    "L": "line-L",
    "N": "line-N",
    "Q": "line-Q",
    "R": "line-R",
    "W": "line-W",
    "S": "line-S",
    "SI": "line-sir",
}

# Aggregate lines — used for summary/mode stats (have both service + ridership)
SUMMARY_LINE_IDS = ["line-subway", "line-bus", "line-lirr", "line-mnr", "line-sir"]

# Individual subway lines — service data only, no ridership
SUBWAY_LINE_IDS = [lid for lid in SUBWAY_ROUTE_TO_LINE.values() if lid != "line-sir"]

# ---------------------------------------------------------------------------
# Line metadata
# ---------------------------------------------------------------------------

LINE_METADATA: dict[str, dict] = {
    # Aggregate lines
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
    # Individual subway lines
    "line-1": {
        "shortName": "1",
        "longName": "Broadway-7th Avenue Local",
        "lineKind": "rapid-transit",
    },
    "line-2": {
        "shortName": "2",
        "longName": "7th Avenue Express",
        "lineKind": "rapid-transit",
    },
    "line-3": {
        "shortName": "3",
        "longName": "7th Avenue Express",
        "lineKind": "rapid-transit",
    },
    "line-4": {
        "shortName": "4",
        "longName": "Lexington Avenue Express",
        "lineKind": "rapid-transit",
    },
    "line-5": {
        "shortName": "5",
        "longName": "Lexington Avenue Express",
        "lineKind": "rapid-transit",
    },
    "line-6": {
        "shortName": "6",
        "longName": "Lexington Avenue Local",
        "lineKind": "rapid-transit",
    },
    "line-7": {
        "shortName": "7",
        "longName": "Flushing Local/Express",
        "lineKind": "rapid-transit",
    },
    "line-A": {
        "shortName": "A",
        "longName": "8th Avenue Express",
        "lineKind": "rapid-transit",
    },
    "line-C": {
        "shortName": "C",
        "longName": "8th Avenue Local",
        "lineKind": "rapid-transit",
    },
    "line-E": {
        "shortName": "E",
        "longName": "8th Avenue Local",
        "lineKind": "rapid-transit",
    },
    "line-B": {
        "shortName": "B",
        "longName": "6th Avenue Express",
        "lineKind": "rapid-transit",
    },
    "line-D": {
        "shortName": "D",
        "longName": "6th Avenue Express",
        "lineKind": "rapid-transit",
    },
    "line-F": {
        "shortName": "F",
        "longName": "6th Avenue Local",
        "lineKind": "rapid-transit",
    },
    "line-M": {
        "shortName": "M",
        "longName": "6th Avenue Local",
        "lineKind": "rapid-transit",
    },
    "line-G": {
        "shortName": "G",
        "longName": "Brooklyn-Queens Crosstown",
        "lineKind": "rapid-transit",
    },
    "line-J": {
        "shortName": "J",
        "longName": "Nassau Street Local",
        "lineKind": "rapid-transit",
    },
    "line-Z": {
        "shortName": "Z",
        "longName": "Nassau Street Express",
        "lineKind": "rapid-transit",
    },
    "line-L": {
        "shortName": "L",
        "longName": "14th Street-Canarsie Local",
        "lineKind": "rapid-transit",
    },
    "line-N": {
        "shortName": "N",
        "longName": "Broadway Express",
        "lineKind": "rapid-transit",
    },
    "line-Q": {
        "shortName": "Q",
        "longName": "2nd Avenue/Broadway Express",
        "lineKind": "rapid-transit",
    },
    "line-R": {
        "shortName": "R",
        "longName": "Broadway Local",
        "lineKind": "rapid-transit",
    },
    "line-W": {
        "shortName": "W",
        "longName": "Broadway Local",
        "lineKind": "rapid-transit",
    },
    "line-S": {"shortName": "S", "longName": "Shuttle", "lineKind": "rapid-transit"},
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
