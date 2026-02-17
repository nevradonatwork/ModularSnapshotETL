import os

import pandas as pd
import pytest

from main import discover_cities
from tests.conftest import COLUMNS, SAMPLE_ROWS


def _create_city_dataset(base_dir, city_name, rows=SAMPLE_ROWS):
    """Create a dataset/<city>/listings.csv.gz structure."""
    city_dir = base_dir / city_name
    city_dir.mkdir(parents=True)
    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_csv(city_dir / "listings.csv.gz", index=False, compression="gzip")
    return city_dir


class TestDiscoverCities:
    def test_finds_single_city(self, tmp_path):
        _create_city_dataset(tmp_path, "new-york")
        assert discover_cities(str(tmp_path)) == ["new-york"]

    def test_finds_multiple_cities_sorted(self, tmp_path):
        _create_city_dataset(tmp_path, "chicago")
        _create_city_dataset(tmp_path, "new-york")
        _create_city_dataset(tmp_path, "seattle")
        assert discover_cities(str(tmp_path)) == ["chicago", "new-york", "seattle"]

    def test_ignores_directories_without_listings(self, tmp_path):
        _create_city_dataset(tmp_path, "new-york")
        (tmp_path / "empty-city").mkdir()
        assert discover_cities(str(tmp_path)) == ["new-york"]

    def test_returns_empty_for_missing_directory(self, tmp_path):
        assert discover_cities(str(tmp_path / "nonexistent")) == []

    def test_returns_empty_for_empty_directory(self, tmp_path):
        assert discover_cities(str(tmp_path)) == []
