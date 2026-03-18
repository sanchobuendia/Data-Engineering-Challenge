from __future__ import annotations

from datetime import datetime
import hashlib


def coordinate_cell(lat: float, lon: float, precision: int = 3) -> str:
    return f"{round(lat, precision):.{precision}f},{round(lon, precision):.{precision}f}"


def derive_time_of_day(dt: datetime) -> str:
    hour = dt.hour
    if 0 <= hour <= 5:
        return "night"
    if 6 <= hour <= 11:
        return "morning"
    if 12 <= hour <= 17:
        return "afternoon"
    return "evening"


def build_event_hash(payload: dict) -> str:
    ordered = "|".join(str(payload.get(key, "")) for key in sorted(payload.keys()))
    return hashlib.sha256(ordered.encode("utf-8")).hexdigest()
