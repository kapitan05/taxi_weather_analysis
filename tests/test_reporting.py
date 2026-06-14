"""Unit tests for the reporting layer (pure shaping + figure builders).

No database: shaping functions are fed synthetic DataFrames that mimic the
rpt.* view output, and the views.sql file is checked structurally.
"""

import matplotlib
import pandas as pd
import pytest

matplotlib.use("Agg")
from matplotlib.figure import Figure  # noqa: E402

from src.reporting import render  # noqa: E402
from src.reporting.render import (  # noqa: E402
    PRECIP_BAND_ORDER,
    SEASON_ORDER,
    VIEWS_SQL,
    fig_daily_kpi,
    fig_hourly_weather,
    fig_monthly_trips,
    fig_precip_trips,
    fig_seasonal,
    fig_temp_duration,
    reconcile_total,
    shape_daily_kpi,
    shape_hourly_weather,
    shape_monthly_trips,
    shape_precip_trips,
    shape_seasonal,
)

# --- shaping ---------------------------------------------------------------


def test_shape_precip_trips_orders_bands(precip_df):
    out = shape_precip_trips(precip_df)
    assert list(out["precip_band"]) == PRECIP_BAND_ORDER
    assert "avg_trip_count" in out.columns


def test_shape_monthly_trips_is_chronological(monthly_df):
    out = shape_monthly_trips(monthly_df)
    assert list(out["month"]) == [1, 2, 3]
    assert list(out["period"]) == ["2023-01", "2023-02", "2023-03"]


def test_shape_daily_kpi_aggregates_by_borough(daily_kpi_df):
    out = shape_daily_kpi(daily_kpi_df)
    manhattan = out.loc[out["pickup_borough"] == "Manhattan", "trip_count"].iloc[0]
    assert manhattan == 110  # 50 + 60 across two days
    # Sorted descending by trip_count.
    assert out["trip_count"].is_monotonic_decreasing


def test_shape_seasonal_orders_seasons(seasonal_df):
    out = shape_seasonal(seasonal_df)
    assert list(out["season"]) == SEASON_ORDER


def test_shape_hourly_weather_averages_per_hour(hourly_weather_df):
    out = shape_hourly_weather(hourly_weather_df)
    assert list(out["hour"]) == [9, 10]
    assert out.loc[out["hour"] == 9, "temperature"].iloc[0] == 6.0  # mean(5,7)


# --- reconciliation --------------------------------------------------------


def test_reconcile_total_passes_when_sums_match(daily_kpi_df):
    out = shape_daily_kpi(daily_kpi_df)
    assert reconcile_total(out, daily_kpi_df["trip_count"].sum(), "trip_count")


def test_reconcile_total_fails_on_mismatch(daily_kpi_df):
    out = shape_daily_kpi(daily_kpi_df)
    assert not reconcile_total(out, daily_kpi_df["trip_count"].sum() + 1, "trip_count")


# --- figure builders -------------------------------------------------------


@pytest.mark.parametrize(
    "builder, fixture_name",
    [
        (fig_precip_trips, "precip_df"),
        (fig_temp_duration, "temp_duration_df"),
        (fig_monthly_trips, "monthly_df"),
        (fig_daily_kpi, "daily_kpi_df"),
        (fig_seasonal, "seasonal_df"),
        (fig_hourly_weather, "hourly_weather_df"),
    ],
)
def test_figure_builders_return_nonempty_figure(builder, fixture_name, request):
    df = request.getfixturevalue(fixture_name)
    fig = builder(df)
    assert isinstance(fig, Figure)
    assert len(fig.axes) >= 1
    assert len(fig.axes[0].patches) + len(fig.axes[0].lines) > 0


# --- views.sql structural checks ------------------------------------------


def test_views_sql_exists_and_creates_rpt_schema():
    sql = VIEWS_SQL.read_text()
    assert "CREATE SCHEMA IF NOT EXISTS rpt" in sql


def test_every_report_view_is_defined_in_sql():
    sql = VIEWS_SQL.read_text().lower()
    for view, _builder, _filename in render.REPORTS.values():
        assert f"create or replace view {view.lower()}" in sql


def test_views_join_current_location_version():
    # SCD2 correctness: location joins must filter to the live version.
    sql = " ".join(VIEWS_SQL.read_text().lower().split())
    assert "f.pu_location_key = pu.location_id and pu.is_current" in sql


def test_reports_registry_has_six_reports():
    assert len(render.REPORTS) == 6
