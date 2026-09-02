from __future__ import annotations

from dockwatch.poller.gbfs_poller import diff_stations


def test_diff_stations_returns_only_the_station_that_changed() -> None:
    previous = {
        "1": {"station_id": "1", "num_bikes_available": 5, "num_docks_available": 10, "last_reported": 100},
        "2": {"station_id": "2", "num_bikes_available": 3, "num_docks_available": 12, "last_reported": 200},
    }
    current = {
        "1": {"station_id": "1", "num_bikes_available": 5, "num_docks_available": 10, "last_reported": 100},
        "2": {"station_id": "2", "num_bikes_available": 2, "num_docks_available": 13, "last_reported": 250},
    }

    changed = diff_stations(previous, current)

    assert [s["station_id"] for s in changed] == ["2"]


def test_diff_stations_ignores_a_station_that_dropped_out_of_the_feed() -> None:
    previous = {
        "1": {"station_id": "1", "num_bikes_available": 5, "num_docks_available": 10, "last_reported": 100},
        "2": {"station_id": "2", "num_bikes_available": 3, "num_docks_available": 12, "last_reported": 200},
    }
    current = {
        "2": {"station_id": "2", "num_bikes_available": 3, "num_docks_available": 12, "last_reported": 200},
    }

    changed = diff_stations(previous, current)

    # diff_stations only walks `current`, so a station missing from this
    # poll (station "1") is neither reported as changed nor errors out —
    # its disappearance is simply not published.
    assert changed == []


def test_diff_stations_treats_last_reported_only_change_as_changed() -> None:
    previous = {
        "1": {"station_id": "1", "num_bikes_available": 5, "num_docks_available": 10, "last_reported": 100},
    }
    current = {
        # Same bike/dock counts, later last_reported — e.g. a lock/unlock
        # with no net change. Still counts as changed: freshness tracking
        # downstream cares about this event even though the counts don't move.
        "1": {"station_id": "1", "num_bikes_available": 5, "num_docks_available": 10, "last_reported": 150},
    }

    changed = diff_stations(previous, current)

    assert [s["station_id"] for s in changed] == ["1"]
