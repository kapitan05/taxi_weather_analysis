
import logging
from dataclasses import dataclass
from datetime import date

logger = logging.getLogger(__name__)

# Candidate TLC service_zone values to flip between (real TLC categories).
_SERVICE_ZONES = ("Boro Zone", "Yellow Zone", "EWR", "Airports")


@dataclass(frozen=True)
class SimulatedChange:

    location_id: int
    new_service_zone: str
    effective_date: date


def planned_changes(year: int) -> list[SimulatedChange]:
    
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
