from dataclasses import dataclass
from typing import Callable

from gtfs_models import GtfsRoute


@dataclass
class AgencyFeedConfig:
    agency_id: str
    display_name: str
    feed_url: str
    mobilitydb_id: str
    route_filter: Callable[[GtfsRoute], bool] | None = None


MOBILITYDB_BASE_URL = "https://api.mobilitydatabase.org/v1"

MTA_FEEDS = [
    AgencyFeedConfig(
        agency_id="nyct_subway",
        display_name="NYC Subway",
        feed_url="http://web.mta.info/developers/data/nyct/subway/google_transit.zip",
        mobilitydb_id="mdb-516",
    ),
    AgencyFeedConfig(
        agency_id="nyct_bus_bronx",
        display_name="NYC Bus - Bronx",
        feed_url="http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip",
        mobilitydb_id="mdb-528",
    ),
    AgencyFeedConfig(
        agency_id="nyct_bus_brooklyn",
        display_name="NYC Bus - Brooklyn",
        feed_url="http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip",
        mobilitydb_id="mdb-512",
    ),
    AgencyFeedConfig(
        agency_id="nyct_bus_manhattan",
        display_name="NYC Bus - Manhattan",
        feed_url="http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip",
        mobilitydb_id="mdb-513",
    ),
    AgencyFeedConfig(
        agency_id="nyct_bus_queens",
        display_name="NYC Bus - Queens",
        feed_url="http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip",
        mobilitydb_id="mdb-520",
    ),
    AgencyFeedConfig(
        agency_id="nyct_bus_staten_island",
        display_name="NYC Bus - Staten Island",
        feed_url="http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip",
        mobilitydb_id="mdb-514",
    ),
    AgencyFeedConfig(
        agency_id="mta_bus",
        display_name="MTA Bus Company",
        feed_url="http://web.mta.info/developers/data/busco/google_transit.zip",
        mobilitydb_id="mdb-510",
    ),
    AgencyFeedConfig(
        agency_id="lirr",
        display_name="Long Island Rail Road",
        feed_url="http://web.mta.info/developers/data/lirr/google_transit.zip",
        mobilitydb_id="mdb-507",
    ),
    AgencyFeedConfig(
        agency_id="metro_north",
        display_name="Metro-North Railroad",
        feed_url="http://web.mta.info/developers/data/mnr/google_transit.zip",
        mobilitydb_id="",  # not available on MobilityDatabase
    ),
]

DYNAMODB_TABLE_NAME = "ScheduledServiceDaily"
