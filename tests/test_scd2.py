
from datetime import date, timedelta

from src.transform.dimensions import (
    SCD2_EPOCH,
    LocationVersion,
    compute_scd2_changes,
)


def test_unchanged_snapshot_is_noop(current_locations, effective):
    incoming = list(current_locations)  # identical
    plan = compute_scd2_changes(current_locations, incoming, effective)
    assert plan.closes == []
    assert plan.inserts == []


def test_new_location_inserts_version_one(current_locations, effective):
    incoming = current_locations + [
        LocationVersion(99, "Bronx", "Mott Haven", "Boro Zone")
    ]
    plan = compute_scd2_changes(current_locations, incoming, effective)
    assert plan.closes == []
    assert len(plan.inserts) == 1
    loc_id, _, _, svc, valid_from, version = plan.inserts[0]
    assert loc_id == 99
    assert svc == "Boro Zone"
    assert valid_from == SCD2_EPOCH
    assert version == 1


def test_service_zone_change_closes_and_versions(current_locations, effective):
    incoming = [
        LocationVersion(1, "Manhattan", "Midtown", "Yellow Zone", version=1),
        LocationVersion(2, "Brooklyn", "Park Slope", "Yellow Zone", version=1),  # changed
        LocationVersion(3, "Queens", "JFK", "Airports", version=2),
    ]
    plan = compute_scd2_changes(current_locations, incoming, effective)

    assert plan.closes == [(2, effective - timedelta(days=1))]
    assert len(plan.inserts) == 1
    loc_id, _, _, svc, valid_from, version = plan.inserts[0]
    assert loc_id == 2
    assert svc == "Yellow Zone"
    assert valid_from == effective
    assert version == 2  # previous version (1) + 1


def test_change_only_affects_changed_rows(current_locations, effective):
    incoming = [
        LocationVersion(1, "Manhattan", "Midtown", "Boro Zone", version=1),  # changed
        LocationVersion(2, "Brooklyn", "Park Slope", "Boro Zone", version=1),
        LocationVersion(3, "Queens", "JFK", "Airports", version=2),
    ]
    plan = compute_scd2_changes(current_locations, incoming, effective)
    assert [c[0] for c in plan.closes] == [1]
    assert [i[0] for i in plan.inserts] == [1]


def test_borough_or_zone_change_also_tracked(current_locations, effective):
    incoming = [
        LocationVersion(1, "Manhattan", "Upper West", "Yellow Zone", version=1),  # zone changed
        LocationVersion(2, "Brooklyn", "Park Slope", "Boro Zone", version=1),
        LocationVersion(3, "Queens", "JFK", "Airports", version=2),
    ]
    plan = compute_scd2_changes(current_locations, incoming, effective)
    assert [c[0] for c in plan.closes] == [1]


def test_new_version_valid_from_is_after_closed_valid_to(current_locations, effective):
    incoming = [
        LocationVersion(1, "Manhattan", "Midtown", "Boro Zone", version=1),
        LocationVersion(2, "Brooklyn", "Park Slope", "Boro Zone", version=1),
        LocationVersion(3, "Queens", "JFK", "Airports", version=2),
    ]
    plan = compute_scd2_changes(current_locations, incoming, effective)
    closed_valid_to = plan.closes[0][1]
    new_valid_from = plan.inserts[0][4]
    # No gap and no overlap: old closes the day before the new one starts.
    assert closed_valid_to == new_valid_from - timedelta(days=1)


def test_empty_current_inserts_all_as_v1():
    incoming = [LocationVersion(5, "Bronx", "Hub", "Boro Zone")]
    plan = compute_scd2_changes([], incoming, date(2023, 1, 1))
    assert plan.closes == []
    assert plan.inserts[0][5] == 1  # version
    assert plan.inserts[0][4] == SCD2_EPOCH
