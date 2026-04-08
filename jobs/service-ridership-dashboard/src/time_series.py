from datetime import date, timedelta
from typing import Callable, Optional, TypeVar

Entry = TypeVar("Entry")
WeeklyMedianTimeSeries = dict[str, float]


def _get_monday_of_week(d: date) -> date:
    """Get the Monday of the week containing the given date."""
    return d - timedelta(days=d.weekday())


def _date_to_string(d: date) -> str:
    return d.isoformat()


def _date_range(start: date, end: date):
    """Yield each date from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _bucket_by_week(entries: dict[date, Entry]) -> dict[date, list[Entry]]:
    """Group entries by the Monday of the week they belong to."""
    buckets: dict[date, list[Entry]] = {}
    for d, entry in entries.items():
        monday = _get_monday_of_week(d)
        buckets.setdefault(monday, [])
        buckets[monday].append(entry)
    return buckets


def _iterate_mondays(
    entries: dict[date, Entry],
    start_date: date,
    max_end_date: date,
):
    """Yield (date, entry) pairs for each date in entries within the range."""
    if not entries:
        return
    max_found_date = max(entries.keys())
    end_date = min(max_end_date, max_found_date)
    for d in _date_range(start_date, end_date):
        if d in entries:
            yield d, entries[d]


def get_weekly_median_time_series(
    entries: dict[date, Entry],
    entry_value_getter: Callable[[Entry], float],
    start_date: date,
    max_end_date: date,
) -> WeeklyMedianTimeSeries:
    """Compute a weekly median time series from daily (or weekly) entries.

    Groups entries by their Monday-of-week, then takes the median value
    for each week. Works for both daily data (multiple entries per week ->
    true median) and weekly data (one entry per week -> that value).
    """
    weekly_buckets = _bucket_by_week(entries)
    weekly_medians: WeeklyMedianTimeSeries = {}
    for week_start, week_entries in _iterate_mondays(
        weekly_buckets, start_date, max_end_date
    ):
        week_values = sorted(entry_value_getter(e) for e in week_entries)
        weekly_medians[_date_to_string(week_start)] = week_values[len(week_values) // 2]
    return weekly_medians


def merge_weekly_median_time_series(
    many_series: list[WeeklyMedianTimeSeries],
) -> WeeklyMedianTimeSeries:
    """Merge multiple weekly time series by summing values for each week."""
    merged: WeeklyMedianTimeSeries = {}
    for series in many_series:
        for week_start, value in series.items():
            merged.setdefault(week_start, 0)
            merged[week_start] += value
    return merged


def get_weekly_median_time_series_entry_for_date(
    series: WeeklyMedianTimeSeries, d: date
) -> Optional[float]:
    """Look up the value for the week containing the given date."""
    monday = _get_monday_of_week(d)
    return series.get(_date_to_string(monday))


def get_latest_weekly_median_time_series_entry(
    series: WeeklyMedianTimeSeries,
) -> Optional[float]:
    """Get the value of the most recent week in the series."""
    if not series:
        return None
    latest_date = max(series.keys())
    return series.get(latest_date)
