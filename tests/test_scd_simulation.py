
from datetime import date

from src.transform.scd_simulation import (
    overrides_effective_on,
    planned_changes,
)


def test_planned_changes_are_deterministic():
    assert planned_changes(2023) == planned_changes(2023)


def test_planned_changes_sorted_by_effective_date():
    changes = planned_changes(2023)
    dates = [c.effective_date for c in changes]
    assert dates == sorted(dates)


def test_effective_dates_inside_year():
    for c in planned_changes(2023):
        assert c.effective_date.year == 2023


def test_overrides_empty_before_first_change():
    changes = planned_changes(2023)
    assert overrides_effective_on(changes, date(2023, 1, 1)) == {}


def test_overrides_accumulate_over_time():
    changes = planned_changes(2023)
    mid = overrides_effective_on(changes, date(2023, 7, 1))
    end = overrides_effective_on(changes, date(2023, 12, 31))
    # More changes have taken effect by year end than by mid-year.
    assert set(mid).issubset(set(end))
    assert len(end) >= len(mid) > 0


def test_override_maps_location_to_new_service_zone():
    changes = planned_changes(2023)
    overrides = overrides_effective_on(changes, date(2023, 12, 31))
    # Every override value is a non-empty string keyed by an int location_id.
    assert all(isinstance(k, int) and v for k, v in overrides.items())


def test_latest_change_wins_for_same_location():
    from src.transform.scd_simulation import SimulatedChange

    changes = [
        SimulatedChange(10, "Boro Zone", date(2023, 2, 1)),
        SimulatedChange(10, "Yellow Zone", date(2023, 8, 1)),
    ]
    assert overrides_effective_on(changes, date(2023, 12, 31)) == {10: "Yellow Zone"}
    assert overrides_effective_on(changes, date(2023, 3, 1)) == {10: "Boro Zone"}
