"""Synthetic SCD2 changes for dim_location.

The real TLC zone lookup is a static reference set: service_zone never changes.
To exercise (and demonstrate) the SCD2 type-2 behaviour of dwh.dim_location we
inject deterministic, clearly-synthetic service_zone reclassifications with
effective dates that fall inside the loaded period.

Everything here is pure and seeded, so the same inputs always yield the same
changes — which keeps the warehouse reproducible and the tests stable.
"""

import logging
from dataclasses import dataclass
from datetime import date

logger = logging.getLogger(__name__)

# Candidate TLC service_zone values to flip between (real TLC categories).
_SERVICE_ZONES = ("Boro Zone", "Yellow Zone", "EWR", "Airports")


@dataclass(frozen=True)
class SimulatedChange:
    """A single synthetic service_zone reclassification."""

    location_id: int
    new_service_zone: str
    effective_date: date


def planned_changes(year: int) -> list[SimulatedChange]:
    """Deterministic set of synthetic service_zone changes for a given year.

    A small, fixed set of zones is reclassified mid-year so that loads covering
    those months produce new SCD2 versions. Returned sorted by effective_date.
    """
    changes = [
        SimulatedChange(100, "Yellow Zone", date(year, 4, 1)),
        SimulatedChange(132, "Yellow Zone", date(year, 7, 1)),
        SimulatedChange(138, "Boro Zone", date(year, 7, 1)),
        SimulatedChange(7, "Yellow Zone", date(year, 10, 1)),
    ]
    return sorted(changes, key=lambda c: (c.effective_date, c.location_id))


def overrides_effective_on(
    changes: list[SimulatedChange], as_of: date
) -> dict[int, str]:
    """Collapse the changes effective on/before `as_of` into a service_zone map.

    If a zone has multiple changes by `as_of`, the latest-effective one wins.
    The result is suitable as `service_zone_overrides` for load_dim_location.
    """
    latest: dict[int, SimulatedChange] = {}
    for ch in sorted(changes, key=lambda c: c.effective_date):
        if ch.effective_date <= as_of:
            latest[ch.location_id] = ch
    overrides = {loc_id: ch.new_service_zone for loc_id, ch in latest.items()}
    logger.info(
        "Computed simulated service_zone overrides",
        extra={"as_of": as_of.isoformat(), "count": len(overrides)},
    )
    return overrides
