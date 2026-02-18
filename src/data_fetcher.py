"""
Inside Airbnb Data Fetcher Agent.

Discovers available cities and snapshots from insideairbnb.com/get-the-data,
downloads listings.csv.gz, neighbourhoods.csv, and neighbourhoods.geojson,
and places them in the correct folder structure for the ETL pipeline.

Strategy:
    1. Try scraping the live page (works if server-rendered HTML)
    2. Fall back to a comprehensive built-in catalog of ~100 cities with
       known URL paths, then probe data.insideairbnb.com for latest dates

Usage (CLI):
    python -m src.data_fetcher                     # interactive: pick city
    python -m src.data_fetcher --city new-york      # specific city
    python -m src.data_fetcher --list               # list available cities

Usage (library):
    from src.data_fetcher import fetch_city_data
    files = fetch_city_data("new-york", dest_dir="dataset")
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
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
# Comprehensive city catalog — URL path components for data.insideairbnb.com
#
# Format: (city_slug, country_path, region_path, city_path, label)
# URL pattern: http://data.insideairbnb.com/{country}/{region}/{city}/{date}/...
# ---------------------------------------------------------------------------

CITY_CATALOG: list[tuple[str, str, str, str, str]] = [
    # ===== UNITED STATES =====
    ("new-york-city",      "united-states", "ny",  "new-york-city",      "New York City, US"),
    ("los-angeles",        "united-states", "ca",  "los-angeles",        "Los Angeles, US"),
    ("san-francisco",      "united-states", "ca",  "san-francisco",      "San Francisco, US"),
    ("san-diego",          "united-states", "ca",  "san-diego",          "San Diego, US"),
    ("san-mateo-county",   "united-states", "ca",  "san-mateo-county",   "San Mateo County, US"),
    ("santa-clara-county", "united-states", "ca",  "santa-clara-county", "Santa Clara County, US"),
    ("santa-cruz-county",  "united-states", "ca",  "santa-cruz-county",  "Santa Cruz County, US"),
    ("oakland",            "united-states", "ca",  "oakland",            "Oakland, US"),
    ("pacific-grove",      "united-states", "ca",  "pacific-grove",      "Pacific Grove, US"),
    ("chicago",            "united-states", "il",  "chicago",            "Chicago, US"),
    ("austin",             "united-states", "tx",  "austin",             "Austin, US"),
    ("dallas",             "united-states", "tx",  "dallas",             "Dallas, US"),
    ("fort-worth",         "united-states", "tx",  "fort-worth",         "Fort Worth, US"),
    ("boston",              "united-states", "ma",  "boston",              "Boston, US"),
    ("cambridge",          "united-states", "ma",  "cambridge",          "Cambridge, US"),
    ("seattle",            "united-states", "wa",  "seattle",            "Seattle, US"),
    ("washington-dc",      "united-states", "dc",  "washington-dc",      "Washington D.C., US"),
    ("portland",           "united-states", "or",  "portland",           "Portland, US"),
    ("denver",             "united-states", "co",  "denver",             "Denver, US"),
    ("nashville",          "united-states", "tn",  "nashville",          "Nashville, US"),
    ("new-orleans",        "united-states", "la",  "new-orleans",        "New Orleans, US"),
    ("asheville",          "united-states", "nc",  "asheville",          "Asheville, US"),
    ("columbus",           "united-states", "oh",  "columbus",           "Columbus, US"),
    ("cleveland",          "united-states", "oh",  "cleveland",          "Cleveland, US"),
    ("hawaii",             "united-states", "hi",  "hawaii",             "Hawaii, US"),
    ("jersey-city",        "united-states", "nj",  "jersey-city",        "Jersey City, US"),
    ("newark",             "united-states", "nj",  "newark",             "Newark, US"),
    ("minneapolis",        "united-states", "mn",  "minneapolis",        "Minneapolis, US"),
    ("saint-paul",         "united-states", "mn",  "twin-cities-msa",    "Twin Cities MSA, US"),
    ("rhode-island",       "united-states", "ri",  "rhode-island",       "Rhode Island, US"),
    ("salem",              "united-states", "or",  "salem-or",           "Salem, US"),
    ("rochester",          "united-states", "ny",  "rochester",          "Rochester, US"),
    # ===== CANADA =====
    ("toronto",            "canada",        "on",  "toronto",            "Toronto, Canada"),
    ("montreal",           "canada",        "qc",  "montreal",           "Montreal, Canada"),
    ("vancouver",          "canada",        "bc",  "vancouver",          "Vancouver, Canada"),
    ("victoria",           "canada",        "bc",  "victoria",           "Victoria, Canada"),
    ("ottawa",             "canada",        "on",  "ottawa",             "Ottawa, Canada"),
    ("quebec-city",        "canada",        "qc",  "quebec-city",        "Quebec City, Canada"),
    ("new-brunswick",      "canada",        "nb",  "new-brunswick",      "New Brunswick, Canada"),
    ("winnipeg",           "canada",        "mb",  "winnipeg",           "Winnipeg, Canada"),
    # ===== UNITED KINGDOM =====
    ("london",             "united-kingdom", "england",  "london",           "London, UK"),
    ("manchester",         "united-kingdom", "england",  "manchester",       "Manchester, UK"),
    ("bristol",            "united-kingdom", "england",  "bristol",          "Bristol, UK"),
    ("edinburgh",          "united-kingdom", "scotland", "edinburgh",        "Edinburgh, UK"),
    ("glasgow",            "united-kingdom", "scotland", "glasgow",          "Glasgow, UK"),
    # ===== SPAIN =====
    ("barcelona",          "spain",         "catalonia",          "barcelona",          "Barcelona, Spain"),
    ("madrid",             "spain",         "comunidad-de-madrid","madrid",             "Madrid, Spain"),
    ("malaga",             "spain",         "andalucia",          "malaga",             "Malaga, Spain"),
    ("sevilla",            "spain",         "andalucia",          "sevilla",            "Sevilla, Spain"),
    ("girona",             "spain",         "catalonia",          "girona",             "Girona, Spain"),
    ("mallorca",           "spain",         "islas-baleares",     "mallorca",           "Mallorca, Spain"),
    ("menorca",            "spain",         "islas-baleares",     "menorca",            "Menorca, Spain"),
    ("valencia",           "spain",         "comunitat-valenciana","valencia",           "Valencia, Spain"),
    ("euskadi",            "spain",         "pais-vasco",         "euskadi",            "Basque Country, Spain"),
    # ===== FRANCE =====
    ("paris",              "france",        "ile-de-france",      "paris",              "Paris, France"),
    ("lyon",               "france",        "auvergne-rhone-alpes","lyon",              "Lyon, France"),
    ("bordeaux",           "france",        "nouvelle-aquitaine", "bordeaux",           "Bordeaux, France"),
    # ===== ITALY =====
    ("rome",               "italy",         "lazio",              "rome",               "Rome, Italy"),
    ("milan",              "italy",         "lombardy",           "milan",              "Milan, Italy"),
    ("florence",           "italy",         "tuscany",            "florence",           "Florence, Italy"),
    ("venice",             "italy",         "veneto",             "venice",             "Venice, Italy"),
    ("naples",             "italy",         "campania",           "naples",             "Naples, Italy"),
    ("bologna",            "italy",         "emilia-romagna",     "bologna",            "Bologna, Italy"),
    ("sicily",             "italy",         "sicily",             "sicily",             "Sicily, Italy"),
    ("sardinia",           "italy",         "sardinia",           "sardinia",           "Sardinia, Italy"),
    ("puglia",             "italy",         "puglia",             "puglia",             "Puglia, Italy"),
    ("bergamo",            "italy",         "lombardy",           "bergamo",            "Bergamo, Italy"),
    # ===== GERMANY =====
    ("berlin",             "germany",       "be",                 "berlin",             "Berlin, Germany"),
    ("munich",             "germany",       "by",                 "munich",             "Munich, Germany"),
    # ===== NETHERLANDS =====
    ("amsterdam",          "the-netherlands","north-holland",     "amsterdam",          "Amsterdam, Netherlands"),
    # ===== PORTUGAL =====
    ("lisbon",             "portugal",      "lisbon",             "lisbon",             "Lisbon, Portugal"),
    ("porto",              "portugal",      "norte",              "porto",              "Porto, Portugal"),
    # ===== GREECE =====
    ("athens",             "greece",        "attica",             "athens",             "Athens, Greece"),
    ("crete",              "greece",        "crete",              "crete",              "Crete, Greece"),
    ("thessaloniki",       "greece",        "central-macedonia",  "thessaloniki",       "Thessaloniki, Greece"),
    ("south-aegean",       "greece",        "south-aegean",       "south-aegean",       "South Aegean, Greece"),
    # ===== IRELAND =====
    ("dublin",             "ireland",       "leinster",           "dublin",             "Dublin, Ireland"),
    # ===== BELGIUM =====
    ("brussels",           "belgium",       "bru",                "brussels",           "Brussels, Belgium"),
    ("antwerp",            "belgium",       "vlg",                "antwerp",            "Antwerp, Belgium"),
    # ===== AUSTRIA =====
    ("vienna",             "austria",       "vienna",             "vienna",             "Vienna, Austria"),
    # ===== SWITZERLAND =====
    ("geneva",             "switzerland",   "geneva",             "geneva",             "Geneva, Switzerland"),
    ("zurich",             "switzerland",   "zurich",             "zurich",             "Zurich, Switzerland"),
    # ===== DENMARK =====
    ("copenhagen",         "denmark",       "hovedstaden",        "copenhagen",         "Copenhagen, Denmark"),
    # ===== SWEDEN =====
    ("stockholm",          "sweden",        "stockholms-lan",     "stockholm",          "Stockholm, Sweden"),
    # ===== NORWAY =====
    ("oslo",               "norway",        "oslo",               "oslo",               "Oslo, Norway"),
    ("bergen",             "norway",        "western-norway",     "bergen",             "Bergen, Norway"),
    # ===== CZECH REPUBLIC =====
    ("prague",             "czech-republic","prague",             "prague",             "Prague, Czech Republic"),
    # ===== TURKEY =====
    ("istanbul",           "turkey",        "marmara",            "istanbul",           "Istanbul, Turkey"),
    # ===== AUSTRALIA =====
    ("sydney",             "australia",     "nsw",                "sydney",             "Sydney, Australia"),
    ("melbourne",          "australia",     "vic",                "melbourne",          "Melbourne, Australia"),
    ("northern-rivers",    "australia",     "nsw",                "northern-rivers",    "Northern Rivers, Australia"),
    ("barossa-valley",     "australia",     "sa",                 "barossa-valley",     "Barossa Valley, Australia"),
    ("tasmania",           "australia",     "tas",                "tasmania",           "Tasmania, Australia"),
    ("western-australia",  "australia",     "wa",                 "western-australia",  "Western Australia"),
    # ===== NEW ZEALAND =====
    ("auckland",           "new-zealand",   "auckland",           "auckland",           "Auckland, NZ"),
    # ===== JAPAN =====
    ("tokyo",              "japan",         "kanto",              "tokyo",              "Tokyo, Japan"),
    # ===== CHINA =====
    ("hong-kong",          "china",         "hong-kong",          "hong-kong",          "Hong Kong, China"),
    ("beijing",            "china",         "beijing",            "beijing",            "Beijing, China"),
    ("shanghai",           "china",         "shanghai",           "shanghai",           "Shanghai, China"),
    # ===== THAILAND =====
    ("bangkok",            "thailand",      "central-thailand",   "bangkok",            "Bangkok, Thailand"),
    # ===== SINGAPORE =====
    ("singapore",          "singapore",     "sg",                 "singapore",          "Singapore"),
    # ===== SOUTH AFRICA =====
    ("cape-town",          "south-africa",  "wc",                 "cape-town",          "Cape Town, South Africa"),
    # ===== MEXICO =====
    ("mexico-city",        "mexico",        "df",                 "mexico-city",        "Mexico City, Mexico"),
    # ===== BRAZIL =====
    ("rio-de-janeiro",     "brazil",        "rj",                 "rio-de-janeiro",     "Rio de Janeiro, Brazil"),
    # ===== ARGENTINA =====
    ("buenos-aires",       "argentina",     "ciudad-autonoma-de-buenos-aires", "buenos-aires", "Buenos Aires, Argentina"),
    # ===== COLOMBIA =====
    ("medellin",           "colombia",      "antioquia",          "medellin",           "Medellin, Colombia"),
    # ===== CUBA =====
    ("havana",             "cuba",          "la-habana",          "havana",             "Havana, Cuba"),
]


# ---------------------------------------------------------------------------
# URL builder from catalog
# ---------------------------------------------------------------------------

_DATA_BASE = "http://data.insideairbnb.com"


def _build_urls(country: str, region: str, city: str, snapshot_date: str) -> dict[str, str]:
    """Build the three download URLs from path components."""
    base = f"{_DATA_BASE}/{country}/{region}/{city}/{snapshot_date}"
    return {
        "listings": f"{base}/data/listings.csv.gz",
        "neighbourhoods_csv": f"{base}/visualisations/neighbourhoods.csv",
        "neighbourhoods_geojson": f"{base}/visualisations/neighbourhoods.geojson",
    }


def _probe_date(country: str, region: str, city: str, candidate: str, timeout: int = 8) -> bool:
    """Check if a snapshot date exists by sending a HEAD request for listings.csv.gz."""
    url = f"{_DATA_BASE}/{country}/{region}/{city}/{candidate}/data/listings.csv.gz"
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def _discover_latest_date(country: str, region: str, city: str) -> str | None:
    """Find the latest available snapshot by probing candidate dates in parallel.

    Inside Airbnb publishes on varying days (1st, 5th, 10th, 20th, 29th, etc.).
    We generate candidates across the last 6 months and probe them concurrently
    using a thread pool, so the total time is ~8-10 seconds instead of minutes.
    """
    today = date.today()

    # Generate candidate dates: common publishing days across last 6 months
    probe_days = (1, 2, 3, 4, 5, 8, 10, 12, 15, 18, 20, 22, 25, 27, 28, 29, 30)
    candidates = []
    for months_ago in range(0, 7):
        y = today.year
        m = today.month - months_ago
        while m <= 0:
            m += 12
            y -= 1
        for day in probe_days:
            try:
                candidates.append(date(y, m, day).isoformat())
            except ValueError:
                pass

    # Sort newest first so the first hit is the latest
    candidates.sort(reverse=True)

    # Probe all candidates in parallel
    found_dates: list[str] = []
    with ThreadPoolExecutor(max_workers=20) as pool:
        future_to_date = {
            pool.submit(_probe_date, country, region, city, c): c
            for c in candidates
        }
        for future in as_completed(future_to_date):
            if future.result():
                found_dates.append(future_to_date[future])

    if found_dates:
        # Return the most recent date
        found_dates.sort(reverse=True)
        logger.info("Found %d snapshots for %s, latest: %s", len(found_dates), city, found_dates[0])
        return found_dates[0]
    return None


# ---------------------------------------------------------------------------
# HTML parser for insideairbnb.com/get-the-data (primary method)
# ---------------------------------------------------------------------------

class InsideAirbnbParser(HTMLParser):
    """Parse the Inside Airbnb 'Get the Data' page to extract download links."""

    def __init__(self):
        super().__init__()
        self._links: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "")
            if "data.insideairbnb.com" in href:
                self._links.append(href)

    def build_snapshots(self) -> list[CitySnapshot]:
        """Group collected links into CitySnapshot objects."""
        link_pattern = re.compile(
            r"https?://data\.insideairbnb\.com/"
            r"(?P<country>[^/]+)/(?P<region>[^/]+)/(?P<city>[^/]+)/"
            r"(?P<date>\d{4}-\d{2}-\d{2})/"
            r"(?P<path>.+)"
        )

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
        for (city_slug, snap_date), info in sorted(groups.items()):
            if "listings" not in info["urls"]:
                continue
            snapshots.append(CitySnapshot(
                city_slug=city_slug,
                city_label=city_slug.replace("-", " ").title(),
                country=info["country"],
                snapshot_date=snap_date,
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
            logger.warning("Attempt %d failed for %s: %s. Retrying in %ds...",
                           attempt + 1, url, e, wait)
            time.sleep(wait)
    return b""


# ---------------------------------------------------------------------------
# Discovery: live scrape → catalog fallback
# ---------------------------------------------------------------------------

def _scrape_live_page() -> list[CitySnapshot]:
    """Try to scrape the live Inside Airbnb page."""
    for page_url in [
        "https://insideairbnb.com/get-the-data/",
        "http://insideairbnb.com/get-the-data.html",
    ]:
        try:
            html_bytes = _urlopen_with_retry(page_url, timeout=60)
            html = html_bytes.decode("utf-8", errors="replace")
            parser = InsideAirbnbParser()
            parser.feed(html)
            snapshots = parser.build_snapshots()
            if snapshots:
                logger.info("Live scrape found %d snapshots from %s", len(snapshots), page_url)
                return snapshots
        except Exception as e:
            logger.debug("Live scrape failed for %s: %s", page_url, e)
    return []


def _catalog_snapshots() -> list[CitySnapshot]:
    """Build snapshots from the built-in catalog without date probing.

    Returns entries with placeholder date '0000-00-00' — the caller should
    use probe_catalog_city() or pass directly to fetch_city_data() which
    will probe on demand.
    """
    snapshots = []
    for slug, country, region, city, label in CITY_CATALOG:
        urls = _build_urls(country, region, city, "PLACEHOLDER")
        snapshots.append(CitySnapshot(
            city_slug=slug,
            city_label=label,
            country=country,
            snapshot_date="",
            listings_url="",
            neighbourhoods_csv_url="",
            neighbourhoods_geojson_url="",
        ))
    return snapshots


def probe_catalog_city(slug: str) -> CitySnapshot | None:
    """Find a city in the catalog and probe for its latest snapshot date."""
    for cat_slug, country, region, city, label in CITY_CATALOG:
        if cat_slug == slug:
            logger.info("Probing for latest snapshot of %s ...", slug)
            snap_date = _discover_latest_date(country, region, city)
            if snap_date is None:
                logger.warning("No recent snapshot found for %s", slug)
                return None
            urls = _build_urls(country, region, city, snap_date)
            return CitySnapshot(
                city_slug=slug,
                city_label=label,
                country=country,
                snapshot_date=snap_date,
                listings_url=urls["listings"],
                neighbourhoods_csv_url=urls["neighbourhoods_csv"],
                neighbourhoods_geojson_url=urls["neighbourhoods_geojson"],
            )
    return None


def discover_snapshots() -> list[CitySnapshot]:
    """Discover available cities — live scrape first, catalog fallback.

    Returns a list of CitySnapshot objects. When falling back to catalog,
    snapshot dates are empty; use probe_catalog_city() for a specific city.
    """
    # Try live scrape first
    snapshots = _scrape_live_page()
    if snapshots:
        return snapshots

    # Fall back to built-in catalog
    logger.info("Live scrape returned no results. Using built-in catalog (%d cities).",
                len(CITY_CATALOG))
    return _catalog_snapshots()


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
                chunk = resp.read(1024 * 256)
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

    If no snapshot is provided (or snapshot has no URLs), discovers/probes automatically.

    Returns a dict of {file_type: local_path} for successfully downloaded files.
    """
    # If snapshot has no URLs (catalog fallback), probe for the real date
    if snapshot is None or not snapshot.listings_url:
        probed = probe_catalog_city(city_slug)
        if probed is None:
            # Try live discovery as last resort
            snapshots = _scrape_live_page()
            latest = get_latest_per_city(snapshots)
            probed = latest.get(city_slug)
            if probed is None:
                for slug, snap in latest.items():
                    if city_slug in slug or slug in city_slug:
                        probed = snap
                        break
        if probed is None:
            catalog_slugs = [c[0] for c in CITY_CATALOG]
            raise ValueError(
                f"City '{city_slug}' not found or no recent snapshot available.\n"
                f"Available cities in catalog: {sorted(catalog_slugs)}"
            )
        snapshot = probed

    city_dir = os.path.join(dest_dir, city_slug)
    os.makedirs(city_dir, exist_ok=True)
    downloaded = {}

    if snapshot.listings_url:
        path = _download_file(snapshot.listings_url, os.path.join(city_dir, "listings.csv.gz"))
        if path:
            downloaded["listings"] = path

    if snapshot.neighbourhoods_csv_url:
        path = _download_file(snapshot.neighbourhoods_csv_url, os.path.join(city_dir, "neighbourhoods.csv"))
        if path:
            downloaded["neighbourhoods_csv"] = path

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
    parser.add_argument("--list", action="store_true",
                        help="List available cities and exit")
    parser.add_argument("--city", type=str,
                        help="City slug to download (e.g. new-york-city)")
    parser.add_argument("--all", action="store_true",
                        help="Download all available cities")
    parser.add_argument("--dest", type=str, default="dataset",
                        help="Destination directory (default: dataset)")
    args = parser.parse_args()

    if args.list:
        # Show the full catalog (always available, no network needed)
        print(f"\n{'City Slug':<30} {'Country':<20} {'Label'}")
        print("-" * 75)
        for slug, country, region, city, label in sorted(CITY_CATALOG, key=lambda c: c[0]):
            print(f"{slug:<30} {country:<20} {label}")
        print(f"\n{len(CITY_CATALOG)} cities in catalog.")
        print("Use --city <slug> to download a specific city.")
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

    # Interactive mode — show catalog
    print(f"\n{len(CITY_CATALOG)} cities available:\n")
    sorted_catalog = sorted(CITY_CATALOG, key=lambda c: c[0])
    for i, (slug, country, region, city, label) in enumerate(sorted_catalog, 1):
        print(f"  {i:3d}. {slug:<30} {label}")

    try:
        choice = input(f"\nEnter city number (1-{len(sorted_catalog)}), or 'all': ").strip()
    except (EOFError, KeyboardInterrupt):
        return

    if choice.lower() == "all":
        results = fetch_all_cities(dest_dir=args.dest)
        successes = sum(1 for v in results.values() if "error" not in v)
        print(f"\nDone. {successes}/{len(results)} cities downloaded.")
    elif choice.isdigit() and 1 <= int(choice) <= len(sorted_catalog):
        slug = sorted_catalog[int(choice) - 1][0]
        files = fetch_city_data(slug, dest_dir=args.dest)
        print(f"\nDownloaded {len(files)} files for {slug}:")
        for ftype, path in files.items():
            print(f"  {ftype}: {path}")
    else:
        print("Invalid choice.")


if __name__ == "__main__":
    main()
