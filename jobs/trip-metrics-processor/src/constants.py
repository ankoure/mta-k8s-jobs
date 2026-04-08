from datetime import date, datetime, timedelta
from decimal import Decimal

ALL_ROUTES: list[tuple[str, str | None]] = [
    ("line-1234567s", None),
    ("line-ace", None),
    ("line-bdfm", None),
    ("line-nqrw", None),
    ("line-g", None),
    ("line-jz", None),
    ("line-l", None),
    ("line-sir", None),
    ("line-mnr", None),
    ("line-lirr", None),
]
ALL_LINES: list[str] = [
    "1234567s",
    "ace",
    "bdfm",
    "nqrw",
    "g",
    "jz",
    "l",
    "sir",
    "mnr",
    "lirr",
]

# Route metadata per line group.
# Each sub-route has terminal stop pairs (NB and SB) and track length in miles.
# Distances sourced from nyc-transit-dash/common/constants/station_distances.json.
ROUTE_METADATA = {
    "line-1234567s": {
        "1":  {"stops": [["101N", "142N"], ["142S", "101S"]], "length": Decimal("14.475")},
        "2":  {"stops": [["201N", "257N"], ["257S", "201S"]], "length": Decimal("24.845")},
        "3":  {"stops": [["301N", "257N"], ["257S", "301S"]], "length": Decimal("17.285")},
        "4":  {"stops": [["401N", "250N"], ["250S", "401S"]], "length": Decimal("19.625")},
        "5":  {"stops": [["501N", "247N"], ["247S", "501S"]], "length": Decimal("23.116")},
        "6":  {"stops": [["601N", "640N"], ["640S", "601S"]], "length": Decimal("13.993")},
        "7":  {"stops": [["701N", "726N"], ["726S", "701S"]], "length": Decimal("9.820")},
        "GS": {"stops": [["901N", "902N"], ["902S", "901S"]], "length": Decimal("0.430")},
    },
    "line-ace": {
        "A": {"stops": [["A02N", "A65N"], ["A65S", "A02S"]], "length": Decimal("22.517")},
        "C": {"stops": [["A09N", "A55N"], ["A55S", "A09S"]], "length": Decimal("17.636")},
        "E": {"stops": [["E01N", "F01N"], ["F01S", "E01S"]], "length": Decimal("15.453")},
    },
    "line-bdfm": {
        "B": {"stops": [["D03N", "D40N"], ["D40S", "D03S"]], "length": Decimal("22.901")},
        "D": {"stops": [["D01N", "D43N"], ["D43S", "D01S"]], "length": Decimal("23.591")},
        "F": {"stops": [["F01N", "D43N"], ["D43S", "F01S"]], "length": Decimal("24.991")},
        "M": {"stops": [["M01N", "G08N"], ["G08S", "M01S"]], "length": Decimal("12.774")},
    },
    "line-nqrw": {
        "N": {"stops": [["R01N", "D43N"], ["D43S", "R01S"]], "length": Decimal("18.847")},
        "Q": {"stops": [["Q05N", "D43N"], ["D43S", "Q05S"]], "length": Decimal("16.294")},
        "R": {"stops": [["R45N", "G08N"], ["G08S", "R45S"]], "length": Decimal("19.014")},
        "W": {"stops": [["R01N", "N10N"], ["N10S", "R01S"]], "length": Decimal("18.452")},
    },
    "line-g": {
        "G": {"stops": [["F27N", "G22N"], ["G22S", "F27S"]], "length": Decimal("9.671")},
    },
    "line-jz": {
        "J": {"stops": [["G05N", "M23N"], ["M23S", "G05S"]], "length": Decimal("12.780")},
        "Z": {"stops": [["G05N", "M23N"], ["M23S", "G05S"]], "length": Decimal("12.780")},
    },
    "line-l": {
        "L": {"stops": [["L01N", "L29N"], ["L29S", "L01S"]], "length": Decimal("9.746")},
    },
    "line-sir": {
        "SI": {"stops": [["S09N", "S31N"], ["S31S", "S09S"]], "length": Decimal("13.818")},
    },
    "line-mnr": {},
    "line-lirr": {},
}


def get_route_metadata(line: str, date: date, include_terminals: bool, route: str | None = None):
    """Return combined stop pairs and total track length for a line group."""
    line_routes = ROUTE_METADATA.get(line, {})
    all_stops = []
    total_length = Decimal("0")
    for route_data in line_routes.values():
        all_stops.extend(route_data["stops"])
        total_length += route_data["length"]
    return {"stops": all_stops, "length": total_length}


LINES: list[str] = [
    "line-1234567s",
    "line-ace",
    "line-bdfm",
    "line-nqrw",
    "line-g",
    "line-jz",
    "line-l",
    "line-sir",
    "line-mnr",
    "line-lirr",
]
RIDERSHIP_KEYS = {
    "line-1234567s": "line-1234567s",
    "line-ace": "line-ace",
    "line-bdfm": "line-bdfm",
    "line-nqrw": "line-nqrw",
    "line-g": "line-g",
    "line-jz": "line-jz",
    "line-l": "line-l",
    "line-sir": "line-sir",
    "line-mnr": "line-mnr",
    "line-lirr": "line-lirr",
}

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"
TODAY = datetime.now().date()

ONE_WEEK_AGO_STRING = (TODAY - timedelta(weeks=1)).strftime(DATE_FORMAT_BACKEND)
NINETY_DAYS_AGO_STRING = (TODAY - timedelta(days=90)).strftime(DATE_FORMAT_BACKEND)


DD_URL_AGG_TT = "https://dashboard-api-beta.gtfscast.com/api/aggregate/traveltimes?{parameters}"
DD_URL_SINGLE_TT = "https://dashboard-api-beta.gtfscast.com/api/traveltimes/{date}?{parameters}"
DD_URL_ALERTS = "https://dashboard-api-beta.gtfscast.com/api/alerts/{date}?{parameters}"



def get_monthly_table_update_start():
    """Get 1st of current month"""
    yesterday = datetime.today() - timedelta(days=1)
    first_of_month = datetime(yesterday.year, yesterday.month, 1)
    return first_of_month


def get_weekly_table_update_start():
    """Get Sunday of current week."""
    yesterday = datetime.now() - timedelta(days=1)
    days_since_monday = yesterday.weekday() % 7
    most_recent_monday = yesterday - timedelta(days=days_since_monday)
    return most_recent_monday


# Configuration for aggregate speed table functions
TABLE_MAP = {
    "weekly": {
        "table_name": "DeliveredTripMetricsWeekly",
        "start_date": datetime.strptime("2016-01-11T08:00:00", DATE_FORMAT),  # Start on first Monday with data.
        "update_start": get_weekly_table_update_start(),
    },
    "monthly": {
        "table_name": "DeliveredTripMetricsMonthly",
        "start_date": datetime.strptime("2016-01-01T08:00:00", DATE_FORMAT),  # Start on 1st of first month with data.
        "update_start": get_monthly_table_update_start(),
    },
}


LINE_TO_ROUTE_MAP = {
    "line-1234567s": ["line-1234567s"],
    "line-ace": ["line-ace"],
    "line-bdfm": ["line-bdfm"],
    "line-nqrw": ["line-nqrw"],
    "line-g": ["line-g"],
    "line-jz": ["line-jz"],
    "line-l": ["line-l"],
    "line-sir": ["line-sir"],
    "line-mnr": ["line-mnr"],
    "line-lirr": ["line-lirr"],
}

ALERT_PATTERNS = {
    "disabled_vehicle": [
        "disabled train",
        "disabled trolley",
        "train that was disabled",
        "disabled bus",
        "train being taken out of service",
        "train being removed from service",
    ],
    "signal_problem": [
        "signal problem",
        "signal issue",
        "signal repairs",
        "signal maintenance",
        "signal repair",
        "signal work",
        "signal department",
    ],
    "switch_problem": [
        "switch problem",
        "switch issue",
        "witch problem",
        "witch issue",
        "switching issue",
    ],
    "brake_problem": [
        "brake issue",
        "brake problem",
        "brakes activated",
        "brakes holding",
        "brakes applied",
    ],
    "power_problem": [
        "power problem",
        "power issue",
        "overhead wires",
        "overhead wire",
        "overhear wires",
        "overheard wires",
        "catenary wires",
        "the overhead",
        "wire repair",
        "repairs to the wire",
        "wire maintenance",
        "wire inspection",
        "wire problem",
        "electrical problem",
        "overhead catenary",
        "third rail wiring",
        "power department work",
    ],
    "door_problem": [
        "door problem",
        "door issue",
    ],
    "track_issue": [
        "track issue",
        "track problem",
        "cracked rail",
        "broken rail",
    ],
    "medical_emergency": [
        "medical emergency",
        "ill passenger",
        "medical assistance",
        "medical attention",
        "sick passenger",
    ],
    "flooding": [
        "flooding",
    ],
    "police_activity": [
        "police",
    ],
    "fire": [
        "fire",
        "smoke",
        "burning",
    ],
    "mechanical_problem": [
        "mechanical problem",
        "mechanical issue",
        "motor problem",
        "pantograph problem",
        "pantograph issue",
        "issue with the heating system",
        "air pressure problem",
    ],
    "track_work": [
        "track work",
        "track maintenance",
        "overnight work",
        "track repair",
        "personnel performed maintenance",
        "maintenance work",
        "overnight maintenance",
        "single track",
    ],
    "car_traffic": [
        "unauthorized vehicle on the tracks",
        "vehicle blocking the tracks",
        "auto accident",
        "car on the tracks",
        "car blocking the tracks",
        "car accident",
        "automobile accident",
        "disabled vehicle on the tracks",
        "due to traffic",
        "car in the track area",
        "car blocking the track area",
        "auto that was blocking",
        "auto blocking the track",
        "auto was removed from the track",
        "accident blocking the tracks",
    ],
}
# Initializing all Delay types at 0 for processing
DELAY_BY_TYPE = {
    "disabled_vehicle": 0,
    "signal_problem": 0,
    "power_problem": 0,
    "door_problem": 0,
    "brake_problem": 0,
    "switch_problem": 0,
    "track_issue": 0,
    "mechanical_problem": 0,
    "track_work": 0,
    "car_traffic": 0,
    "police_activity": 0,
    "medical_emergency": 0,
    "fire": 0,
    "flooding": 0,
    "other": 0,
}
