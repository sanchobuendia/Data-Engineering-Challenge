from datetime import datetime

from app.services.utils import coordinate_cell, derive_time_of_day


def test_coordinate_cell_rounding() -> None:
    assert coordinate_cell(40.712776, -74.005974) == "40.713,-74.006"


def test_derive_time_of_day() -> None:
    assert derive_time_of_day(datetime.fromisoformat("2026-03-18T02:00:00")) == "night"
    assert derive_time_of_day(datetime.fromisoformat("2026-03-18T08:00:00")) == "morning"
    assert derive_time_of_day(datetime.fromisoformat("2026-03-18T14:00:00")) == "afternoon"
    assert derive_time_of_day(datetime.fromisoformat("2026-03-18T21:00:00")) == "evening"
