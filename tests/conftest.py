"""Shared fixtures for the pure unit-test suite (no DB, no Spark)."""

from datetime import date

import pandas as pd
import pytest

from src.transform.dimensions import LocationVersion


@pytest.fixture
def current_locations() -> list[LocationVersion]:
    return [
        LocationVersion(1, "Manhattan", "Midtown", "Yellow Zone", version=1),
        LocationVersion(2, "Brooklyn", "Park Slope", "Boro Zone", version=1),
        LocationVersion(3, "Queens", "JFK", "Airports", version=2),
    ]


@pytest.fixture
def effective() -> date:
    return date(2023, 7, 1)


@pytest.fixture
def precip_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "full_date": pd.to_datetime(
                ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"]
            ),
            "daily_precipitation": [0.0, 3.0, 10.0, 25.0],
            "precip_band": ["Dry", "Light", "Moderate", "Heavy"],
            "trip_count": [100, 80, 60, 30],
        }
    )


@pytest.fixture
def temp_duration_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "temp_bucket": [1, 2, 3],
            "time_of_day": ["Morning", "Morning", "Evening"],
            "trip_count": [10, 20, 30],
            "avg_duration_min": [12.0, 15.0, 9.0],
            "avg_temperature": [-5.0, 10.0, 25.0],
        }
    )


@pytest.fixture
def monthly_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2023, 2023, 2023],
            "month": [3, 1, 2],
            "trip_count": [300, 100, 200],
            "total_revenue": [3000.0, 1000.0, 2000.0],
            "avg_distance": [3.0, 2.0, 2.5],
        }
    )


@pytest.fixture
def daily_kpi_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "full_date": pd.to_datetime(["2023-01-01", "2023-01-01", "2023-01-02"]),
            "pickup_borough": ["Manhattan", "Brooklyn", "Manhattan"],
            "trip_count": [50, 20, 60],
            "total_revenue": [500.0, 200.0, 600.0],
            "avg_distance": [2.0, 3.0, 2.5],
            "avg_duration_min": [10.0, 12.0, 11.0],
            "avg_tip": [1.0, 0.5, 1.2],
        }
    )


@pytest.fixture
def seasonal_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "season": ["Summer", "Winter", "Spring", "Autumn"],
            "trip_count": [400, 100, 200, 300],
            "avg_fare": [20.0, 18.0, 19.0, 21.0],
            "avg_distance": [3.0, 2.0, 2.5, 2.8],
            "avg_temperature": [25.0, 0.0, 12.0, 15.0],
        }
    )


@pytest.fixture
def hourly_weather_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "full_date": pd.to_datetime(["2023-01-01"] * 4),
            "hour": [9, 9, 10, 10],
            "temperature": [5.0, 7.0, 9.0, 11.0],
            "precipitation": [0.0, 1.0, 0.0, 2.0],
            "wind_speed": [10.0, 12.0, 8.0, 6.0],
            "condition_name": ["Clear", "Rain", "Clear", "Rain"],
        }
    )
