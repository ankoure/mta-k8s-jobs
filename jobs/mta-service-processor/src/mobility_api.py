"""
MobilityDatabase API client for fetching GTFS feed datasets.

Handles authentication, token refresh, pagination, and rate limiting.
API docs: https://mobilitydata.github.io/mobility-feed-api/SwaggerUI/index.html
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 1  # seconds
BACKOFF_MAX = 30  # seconds
API_TIMEOUT = 30  # seconds


@dataclass
class MobilityDataset:
    id: str
    feed_id: str
    downloaded_at: datetime
    download_url: str
    hash: str


class MobilityApiError(Exception):
    pass


class MobilityApiClient:
    def __init__(self, refresh_token: str, base_url: str):
        self._refresh_token = refresh_token
        self._base_url = base_url.rstrip("/")
        self._access_token: str | None = None

    def _authenticate(self) -> None:
        """Exchange refresh token for an access token."""
        logger.info("Authenticating with MobilityDatabase API")
        resp = requests.post(
            f"{self._base_url}/tokens",
            headers={"Content-Type": "application/json"},
            json={"refresh_token": self._refresh_token},
            timeout=API_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data.get("access_token") or data.get("token")
        if not self._access_token:
            raise MobilityApiError(
                f"No access token in auth response: {list(data.keys())}"
            )
        logger.info("MobilityDatabase authentication successful")

    def _get(self, path: str, params: dict | None = None) -> dict | list:
        """Authenticated GET with token refresh on 401 and backoff on 429."""
        if not self._access_token:
            self._authenticate()

        url = f"{self._base_url}{path}"
        for attempt in range(MAX_RETRIES + 1):
            resp = requests.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=API_TIMEOUT,
            )

            if resp.status_code == 401 and attempt == 0:
                logger.info("Access token expired, re-authenticating")
                self._authenticate()
                continue

            if resp.status_code == 429:
                delay = min(BACKOFF_BASE * (2**attempt), BACKOFF_MAX)
                logger.warning(
                    f"Rate limited, retrying in {delay}s (attempt {attempt + 1})"
                )
                time.sleep(delay)
                continue

            resp.raise_for_status()
            return resp.json()

        raise MobilityApiError(f"Request failed after {MAX_RETRIES} retries: {url}")

    def get_feed_datasets(
        self,
        feed_id: str,
        latest: bool = False,
        limit: int = 100,
        offset: int = 0,
        downloaded_after: str | None = None,
        downloaded_before: str | None = None,
    ) -> list[MobilityDataset]:
        """Get datasets for a feed. Each dataset is a point-in-time GTFS snapshot."""
        params = {"limit": limit, "offset": offset}
        if latest:
            params["latest"] = "true"
        if downloaded_after:
            params["downloaded_after"] = downloaded_after
        if downloaded_before:
            params["downloaded_before"] = downloaded_before

        data = self._get(f"/gtfs_feeds/{feed_id}/datasets", params=params)

        datasets = []
        for item in data:
            download_url = (
                item.get("hosted_url")
                or item.get("download_url")
                or item.get("source_url", "")
            )
            if not download_url:
                continue
            datasets.append(
                MobilityDataset(
                    id=item.get("id", ""),
                    feed_id=item.get("feed_id", feed_id),
                    downloaded_at=datetime.fromisoformat(
                        item["downloaded_at"].replace("Z", "+00:00")
                    ),
                    download_url=download_url,
                    hash=item.get("hash", ""),
                )
            )
        return datasets

    def get_latest_dataset(self, feed_id: str) -> MobilityDataset | None:
        """Get the most recent dataset for a feed."""
        datasets = self.get_feed_datasets(feed_id, latest=True, limit=1)
        return datasets[0] if datasets else None

    def get_datasets_in_range(
        self,
        feed_id: str,
        after: str | None = None,
        before: str | None = None,
    ) -> list[MobilityDataset]:
        """Fetch all datasets within a date range, paginating as needed."""
        all_datasets = []
        offset = 0
        limit = 100
        while True:
            batch = self.get_feed_datasets(
                feed_id,
                limit=limit,
                offset=offset,
                downloaded_after=after,
                downloaded_before=before,
            )
            if not batch:
                break
            all_datasets.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return all_datasets

    def search_feeds(self, provider: str) -> list[dict]:
        """Search for feeds by provider name."""
        return self._get("/gtfs_feeds", params={"provider": provider, "limit": 20})
