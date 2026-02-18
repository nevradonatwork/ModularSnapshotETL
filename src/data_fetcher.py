"""
Inside Airbnb Data Fetcher Agent.

Discovers available cities and snapshots from insideairbnb.com/get-the-data,
downloads listings.csv.gz, neighbourhoods.csv, and neighbourhoods.geojson,
and places them in the correct folder structure for the ETL pipeline.

Usage (CLI):
    python -m src.data_fetcher                     # interactive: pick city
    python -m src.data_fetcher --city new-york      # specific city
    python -m src.data_fetcher --list               # list available cities

Usage (library):
    from src.data_fetcher import fetch_city_data
    files = fetch_city_data("new-york", dest_dir="dataset")
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from html.parser import HTMLParser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class CitySnapshot:
    """A single data snapshot for a city."""
    city_slug: str
    city_label: str
    country: str
    snapshot_date: str  # YYYY-MM-DD
    listings_url: str
    neighbourhoods_csv_url: str
    neighbourhoods_geojson_url: str


# ---------------------------------------------------------------------------
# HTML parser for insideairbnb.com/get-the-data
# ---------------------------------------------------------------------------

class InsideAirbnbParser(HTMLParser):
    """Parse the Inside Airbnb 'Get the Data' page to extract download links.

    The page organises cities as <h2>/<h3> headings followed by <table> blocks
    containing <a href="..."> links to data.insideairbnb.com.
    """

    def __init__(self):
        super().__init__()
        self.snapshots: list[CitySnapshot] = []
        self._current_city_label = ""
        self._in_heading = False
        self._links: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in ("h2", "h3"):
            self._in_heading = True
            self._heading_text = ""
        if tag == "a":
            href = dict(attrs).get("href", "")
            if "data.insideairbnb.com" in href:
                self._links.append(href)

    def handle_endtag(self, tag):
        if tag in ("h2", "h3") and self._in_heading:
            self._in_heading = False
            self._current_city_label = self._heading_text.strip()

    def handle_data(self, data):
        if self._in_heading:
            self._heading_text += data

    def build_snapshots(self) -> list[CitySnapshot]:
        """Group collected links into CitySnapshot objects."""
        # Links follow the pattern:
        # http(s)://data.insideairbnb.com/{country}/{region}/{city}/{date}/data/listings.csv.gz
        link_pattern = re.compile(
            r"https?://data\.insideairbnb\.com/"
            r"(?P<country>[^/]+)/(?P<region>[^/]+)/(?P<city>[^/]+)/"
            r"(?P<date>\d{4}-\d{2}-\d{2})/"
            r"(?P<path>.+)"
        )

        # Group links by (city, date)
        groups: dict[tuple[str, str], dict] = {}
        for url in self._links:
            m = link_pattern.match(url)
            if not m:
                continue
            key = (m.group("city"), m.group("date"))
            if key not in groups:
                groups[key] = {
                    "country": m.group("country"),
                    "region": m.group("region"),
                    "city": m.group("city"),
                    "date": m.group("date"),
                    "urls": {},
                }
            path = m.group("path")
            if path == "data/listings.csv.gz":
                groups[key]["urls"]["listings"] = url
            elif path == "visualisations/neighbourhoods.csv":
                groups[key]["urls"]["neighbourhoods_csv"] = url
            elif path == "visualisations/neighbourhoods.geojson":
                groups[key]["urls"]["neighbourhoods_geojson"] = url

        snapshots = []
        for (city, date), info in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1]), reverse=True):
            if "listings" not in info["urls"]:
                continue  # skip incomplete snapshots
            snapshots.append(CitySnapshot(
                city_slug=city,
                city_label=city.replace("-", " ").title(),
                country=info["country"],
                snapshot_date=date,
                listings_url=info["urls"].get("listings", ""),
                neighbourhoods_csv_url=info["urls"].get("neighbourhoods_csv", ""),
                neighbourhoods_geojson_url=info["urls"].get("neighbourhoods_geojson", ""),
            ))
        return snapshots


# ---------------------------------------------------------------------------
# Page fetcher with retry
# ---------------------------------------------------------------------------

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _urlopen_with_retry(url: str, retries: int = 3, timeout: int = 30) -> bytes:
    """Fetch a URL with retries and exponential backoff."""
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** (attempt + 1)
            logger.warning("Attempt %d failed for %s: %s. Retrying in %ds...", attempt + 1, url, e, wait)
            time.sleep(wait)
    return b""  # unreachable


def discover_snapshots(page_url: str = "https://insideairbnb.com/get-the-data/") -> list[CitySnapshot]:
    """Fetch the Inside Airbnb 'Get the Data' page and parse available snapshots."""
    logger.info("Fetching %s ...", page_url)
    html_bytes = _urlopen_with_retry(page_url, timeout=60)
    html = html_bytes.decode("utf-8", errors="replace")

    parser = InsideAirbnbParser()
    parser.feed(html)
    snapshots = parser.build_snapshots()

    # If JS-rendered page returned no links, try the old URL as fallback
    if not snapshots and "get-the-data/" in page_url:
        alt_url = page_url.replace("get-the-data/", "get-the-data.html")
        logger.info("No links found, trying fallback URL: %s", alt_url)
        try:
            html_bytes = _urlopen_with_retry(alt_url, timeout=60)
            html = html_bytes.decode("utf-8", errors="replace")
            parser = InsideAirbnbParser()
            parser.feed(html)
            snapshots = parser.build_snapshots()
        except Exception:
            pass

    logger.info("Discovered %d snapshots across %d cities",
                len(snapshots), len({s.city_slug for s in snapshots}))
    return snapshots


def get_latest_per_city(snapshots: list[CitySnapshot]) -> dict[str, CitySnapshot]:
    """From a list of snapshots, keep only the latest date per city."""
    latest: dict[str, CitySnapshot] = {}
    for s in snapshots:
        if s.city_slug not in latest or s.snapshot_date > latest[s.city_slug].snapshot_date:
            latest[s.city_slug] = s
    return latest


# ---------------------------------------------------------------------------
# File downloader
# ---------------------------------------------------------------------------

def _download_file(url: str, dest_path: str) -> str:
    """Download a file from URL to dest_path. Returns dest_path on success."""
    if not url:
        return ""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    logger.info("Downloading %s -> %s", url, dest_path)
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=300) as resp:
        with open(dest_path, "wb") as f:
            while True:
                chunk = resp.read(1024 * 256)  # 256 KB chunks
                if not chunk:
                    break
                f.write(chunk)
    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    logger.info("Downloaded %.1f MB -> %s", size_mb, dest_path)
    return dest_path


def fetch_city_data(
    city_slug: str,
    snapshot: CitySnapshot | None = None,
    dest_dir: str = "dataset",
) -> dict[str, str]:
    """Download all data files for a city and place them in dest_dir/{city_slug}/.

    If no snapshot is provided, discovers available snapshots and picks the latest.

    Returns a dict of {file_type: local_path} for successfully downloaded files.
    """
    if snapshot is None:
        snapshots = discover_snapshots()
        latest = get_latest_per_city(snapshots)
        snapshot = latest.get(city_slug)
        if snapshot is None:
            # Try fuzzy match
            for slug, snap in latest.items():
                if city_slug in slug or slug in city_slug:
                    snapshot = snap
                    break
        if snapshot is None:
            available = sorted(latest.keys())
            raise ValueError(
                f"City '{city_slug}' not found. Available cities: {available}"
            )

    city_dir = os.path.join(dest_dir, city_slug)
    os.makedirs(city_dir, exist_ok=True)
    downloaded = {}

    # listings.csv.gz
    if snapshot.listings_url:
        path = _download_file(snapshot.listings_url, os.path.join(city_dir, "listings.csv.gz"))
        if path:
            downloaded["listings"] = path

    # neighbourhoods.csv
    if snapshot.neighbourhoods_csv_url:
        path = _download_file(snapshot.neighbourhoods_csv_url, os.path.join(city_dir, "neighbourhoods.csv"))
        if path:
            downloaded["neighbourhoods_csv"] = path

    # neighbourhoods.geojson
    if snapshot.neighbourhoods_geojson_url:
        path = _download_file(snapshot.neighbourhoods_geojson_url, os.path.join(city_dir, "neighbourhoods.geojson"))
        if path:
            downloaded["neighbourhoods_geojson"] = path

    logger.info("Fetched %d files for %s (snapshot: %s) -> %s",
                len(downloaded), city_slug, snapshot.snapshot_date, city_dir)
    return downloaded


def fetch_all_cities(
    dest_dir: str = "dataset",
    city_filter: list[str] | None = None,
) -> dict[str, dict[str, str]]:
    """Download the latest snapshot for all (or filtered) cities.

    Returns {city_slug: {file_type: local_path}}.
    """
    snapshots = discover_snapshots()
    latest = get_latest_per_city(snapshots)

    if city_filter:
        latest = {k: v for k, v in latest.items() if k in city_filter}

    results = {}
    for city_slug, snapshot in sorted(latest.items()):
        try:
            downloaded = fetch_city_data(city_slug, snapshot=snapshot, dest_dir=dest_dir)
            results[city_slug] = downloaded
        except Exception as e:
            logger.error("Failed to fetch %s: %s", city_slug, e)
            results[city_slug] = {"error": str(e)}

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(
        description="Fetch Airbnb listing data from Inside Airbnb.",
    )
    parser.add_argument("--list", action="store_true", help="List available cities and exit")
    parser.add_argument("--city", type=str, help="City slug to download (e.g. new-york)")
    parser.add_argument("--all", action="store_true", help="Download all available cities")
    parser.add_argument("--dest", type=str, default="dataset", help="Destination directory (default: dataset)")
    args = parser.parse_args()

    if args.list:
        snapshots = discover_snapshots()
        latest = get_latest_per_city(snapshots)
        print(f"\n{'City Slug':<30} {'Latest Snapshot':<15} {'Country'}")
        print("-" * 65)
        for slug in sorted(latest):
            s = latest[slug]
            print(f"{slug:<30} {s.snapshot_date:<15} {s.country}")
        print(f"\n{len(latest)} cities available.")
        return

    if args.all:
        results = fetch_all_cities(dest_dir=args.dest)
        successes = sum(1 for v in results.values() if "error" not in v)
        print(f"\nDone. {successes}/{len(results)} cities downloaded to {args.dest}/")
        return

    if args.city:
        files = fetch_city_data(args.city, dest_dir=args.dest)
        print(f"\nDownloaded {len(files)} files for {args.city}:")
        for ftype, path in files.items():
            print(f"  {ftype}: {path}")
        return

    # Interactive mode
    print("Discovering available cities from Inside Airbnb...")
    snapshots = discover_snapshots()
    latest = get_latest_per_city(snapshots)
    cities = sorted(latest.keys())

    if not cities:
        print("No cities found. The page may require JavaScript rendering.")
        print("You can manually provide a URL via the dashboard's Load Data page.")
        return

    print(f"\n{len(cities)} cities available:\n")
    for i, slug in enumerate(cities, 1):
        s = latest[slug]
        print(f"  {i:3d}. {slug:<30} ({s.snapshot_date})")

    try:
        choice = input(f"\nEnter city number (1-{len(cities)}), or 'all': ").strip()
    except (EOFError, KeyboardInterrupt):
        return

    if choice.lower() == "all":
        results = fetch_all_cities(dest_dir=args.dest)
        successes = sum(1 for v in results.values() if "error" not in v)
        print(f"\nDone. {successes}/{len(results)} cities downloaded.")
    elif choice.isdigit() and 1 <= int(choice) <= len(cities):
        slug = cities[int(choice) - 1]
        files = fetch_city_data(slug, snapshot=latest[slug], dest_dir=args.dest)
        print(f"\nDownloaded {len(files)} files for {slug}:")
        for ftype, path in files.items():
            print(f"  {ftype}: {path}")
    else:
        print("Invalid choice.")


if __name__ == "__main__":
    main()
