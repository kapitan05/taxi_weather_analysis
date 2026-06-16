
from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import matplotlib

matplotlib.use("Agg") 
import matplotlib.pyplot as plt
import pandas as pd

logger = logging.getLogger(__name__)

REPORT_DIR = Path("reports")
FIGURE_DIR = REPORT_DIR / "figures"
VIEWS_SQL = Path(__file__).with_name("views.sql")

PRECIP_BAND_ORDER = ["Dry", "Light", "Moderate", "Heavy"]
SEASON_ORDER = ["Winter", "Spring", "Summer", "Autumn"]


def shape_precip_trips(df: pd.DataFrame) -> pd.DataFrame:
    """Mean daily trip_count per precipitation band, in severity order."""
    grouped = (
        df.groupby("precip_band", as_index=False)["trip_count"]
        .mean()
        .rename(columns={"trip_count": "avg_trip_count"})
    )
    grouped["precip_band"] = pd.Categorical(
        grouped["precip_band"], categories=PRECIP_BAND_ORDER, ordered=True
    )
    return grouped.sort_values("precip_band").reset_index(drop=True)


def shape_temp_duration(df: pd.DataFrame) -> pd.DataFrame:
    """Average trip duration by temperature, ordered by temperature."""
    grouped = (
        df.groupby("temp_bucket", as_index=False)
        .agg(
            avg_duration_min=("avg_duration_min", "mean"),
            avg_temperature=("avg_temperature", "mean"),
        )
        .sort_values("avg_temperature")
        .reset_index(drop=True)
    )
    return grouped


def shape_monthly_trips(df: pd.DataFrame) -> pd.DataFrame:
    """Monthly trips sorted chronologically with a YYYY-MM label."""
    out = df.sort_values(["year", "month"]).reset_index(drop=True)
    out["period"] = (
        out["year"].astype(int).astype(str)
        + "-"
        + out["month"].astype(int).astype(str).str.zfill(2)
    )
    return out


def shape_daily_kpi(df: pd.DataFrame) -> pd.DataFrame:
    """Total trips and revenue per borough (for the KPI/zone report)."""
    grouped = (
        df.groupby("pickup_borough", as_index=False)
        .agg(trip_count=("trip_count", "sum"), total_revenue=("total_revenue", "sum"))
        .sort_values("trip_count", ascending=False)
        .reset_index(drop=True)
    )
    return grouped


def shape_seasonal(df: pd.DataFrame) -> pd.DataFrame:
    """Seasonal metrics in calendar-season order."""
    out = df.copy()
    out["season"] = pd.Categorical(
        out["season"], categories=SEASON_ORDER, ordered=True
    )
    return out.sort_values("season").reset_index(drop=True)


def shape_hourly_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Average temperature / precipitation / wind per hour of day."""
    grouped = (
        df.groupby("hour", as_index=False)
        .agg(
            temperature=("temperature", "mean"),
            precipitation=("precipitation", "mean"),
            wind_speed=("wind_speed", "mean"),
        )
        .sort_values("hour")
        .reset_index(drop=True)
    )
    return grouped


def reconcile_total(detail: pd.DataFrame, summary_total: float, column: str) -> bool:
    """True if the detail rows sum (within tolerance) to an expected total.

    Used to assert a report does not drop or duplicate measure rows.
    """
    return bool(abs(float(detail[column].sum()) - float(summary_total)) < 1e-6)

def _bar_figure(
    df: pd.DataFrame, x: str, y: str, title: str, xlabel: str, ylabel: str
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(df[x].astype(str), df[y])
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.tight_layout()
    return fig


def _line_figure(
    df: pd.DataFrame, x: str, y: str, title: str, xlabel: str, ylabel: str
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(df[x].astype(str), df[y], marker="o")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    return fig


def fig_precip_trips(df: pd.DataFrame) -> plt.Figure:
    return _bar_figure(
        shape_precip_trips(df),
        "precip_band",
        "avg_trip_count",
        "Trip volume by precipitation band",
        "Precipitation band",
        "Avg trips / day",
    )


def fig_temp_duration(df: pd.DataFrame) -> plt.Figure:
    return _line_figure(
        shape_temp_duration(df),
        "avg_temperature",
        "avg_duration_min",
        "Average trip duration vs temperature",
        "Temperature (°C)",
        "Avg duration (min)",
    )


def fig_monthly_trips(df: pd.DataFrame) -> plt.Figure:
    return _line_figure(
        shape_monthly_trips(df),
        "period",
        "trip_count",
        "Monthly trip trend",
        "Month",
        "Trips",
    )


def fig_daily_kpi(df: pd.DataFrame) -> plt.Figure:
    return _bar_figure(
        shape_daily_kpi(df),
        "pickup_borough",
        "trip_count",
        "Trips by pickup borough",
        "Borough",
        "Trips",
    )


def fig_seasonal(df: pd.DataFrame) -> plt.Figure:
    return _bar_figure(
        shape_seasonal(df),
        "season",
        "trip_count",
        "Seasonal trip comparison",
        "Season",
        "Trips",
    )


def fig_hourly_weather(df: pd.DataFrame) -> plt.Figure:
    return _line_figure(
        shape_hourly_weather(df),
        "hour",
        "temperature",
        "Hourly average temperature",
        "Hour of day",
        "Temperature (°C)",
    )


# name -> (source view, figure builder, output filename)
REPORTS: dict[str, tuple[str, Callable[[pd.DataFrame], plt.Figure], str]] = {
    "Precipitation vs trips": ("rpt.v_precip_trips", fig_precip_trips, "01_precip_trips.png"),
    "Temperature vs duration": ("rpt.v_temp_duration", fig_temp_duration, "02_temp_duration.png"),
    "Monthly trend": ("rpt.v_monthly_trips", fig_monthly_trips, "03_monthly_trips.png"),
    "Daily KPI by borough": ("rpt.v_daily_kpi", fig_daily_kpi, "04_daily_kpi.png"),
    "Seasonal comparison": ("rpt.v_seasonal", fig_seasonal, "05_seasonal.png"),
    "Hourly weather": ("rpt.v_hourly_weather", fig_hourly_weather, "06_hourly_weather.png"),
}


def apply_views() -> None:
    """(Re)create the rpt.* views from views.sql."""
    from src.db.connection import pg_conn

    sql = VIEWS_SQL.read_text()
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
    logger.info("rpt views applied")


def load_view(view: str) -> pd.DataFrame:
    from src.db.connection import pg_conn

    with pg_conn() as conn:
        return pd.read_sql_query(f"SELECT * FROM {view}", conn)


def render_all() -> list[Path]:
    """Build views, render every report to PNG, and a combined PDF."""
    from matplotlib.backends.backend_pdf import PdfPages

    apply_views()
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    pdf_path = REPORT_DIR / "reports.pdf"
    with PdfPages(pdf_path) as pdf:
        for name, (view, builder, filename) in REPORTS.items():
            df = load_view(view)
            fig = builder(df)
            out = FIGURE_DIR / filename
            fig.savefig(out, dpi=120)
            pdf.savefig(fig)
            plt.close(fig)
            written.append(out)
            logger.info("rendered report", extra={"report": name, "rows": len(df)})
    written.append(pdf_path)
    return written


def main() -> None:
    from src.ingest.logging_config import setup_json_logging

    setup_json_logging()
    paths = render_all()
    logger.info("Reporting finished", extra={"outputs": [str(p) for p in paths]})


if __name__ == "__main__":
    main()
