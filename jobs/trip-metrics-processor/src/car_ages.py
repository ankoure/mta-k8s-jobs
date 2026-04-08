from datetime import date
from decimal import Decimal

# Static mapping of car ID ranges to build years, grouped by division.
# Source: https://en.wikipedia.org/wiki/New_York_City_Subway_rolling_stock
CARRIAGE_AGES: dict[str, dict[str, int]] = {
    # A Division (IRT) — serves 1/2/3/4/5/6/7/S
    "A-Division": {
        "1301-1625": 1984,   # R62
        "1651-1875": 1986,   # R62A
        "6301-6780": 2001,   # R142
        "7211-7610": 2002,   # R142A
        "7811-7932": 2012,   # R188 (R142A conversions + new cars)
    },
    # B Division (BMT/IND) — serves A/C/E/B/D/F/M/G/J/Z/L/N/Q/R/W
    "B-Division": {
        "5482-5879": 1977,   # R46
        "2500-2924": 1987,   # R68
        "5001-5200": 1989,   # R68A
        "8101-8272": 2002,   # R143 (primarily L)
        "8313-8653": 2007,   # R160A-1
        "8713-8973": 2008,   # R160B-1
        "9403-9532": 2009,   # R160A-2
        "9533-9622": 2010,   # R160B-2
        "3010-3299": 2018,   # R179
        "4040-4479": 2023,   # R211A (deliveries ongoing)
    },
    # Staten Island Railway
    "SIR": {
        "4480-4579": 2023,   # R211S (deliveries ongoing)
    },
}

# Maps line group to the division key used in CARRIAGE_AGES
LINE_KEY_MAP: dict[str, str] = {
    "line-1234567s": "A-Division",
    "line-ace": "B-Division",
    "line-bdfm": "B-Division",
    "line-nqrw": "B-Division",
    "line-g": "B-Division",
    "line-jz": "B-Division",
    "line-l": "B-Division",
    "line-sir": "SIR",
}


def get_car_build_year(car_id: int, division: str) -> int | None:
    """Look up the build year for a car ID in a given division."""
    division_ages = CARRIAGE_AGES.get(division)
    if not division_ages:
        return None
    for range_str, year in division_ages.items():
        low, high = range_str.split("-")
        if int(low) <= car_id <= int(high):
            return year
    return None


def get_avg_car_age_for_line(current_date: date, line: str) -> Decimal | None:
    """Compute average car age for a line group.

    NOTE: The MTA API currently returns train run numbers in vehicle_consist
    (e.g. "0L 0303 RPY/BED") rather than individual car IDs. Until car-level
    consist data is available, this function returns None. The CARRIAGE_AGES
    data is maintained for future use when the API provides car numbers.
    """
    # TODO: Implement when MTA API provides car-level consist data.
    return None
