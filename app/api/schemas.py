from datetime import date, datetime
from pydantic import BaseModel


class GroupedTripResponse(BaseModel):
    city: str
    region: str | None
    origin_cell: str
    destination_cell: str
    time_of_day: str
    trip_count: int


class WeeklyAverageResponse(BaseModel):
    filter_type: str
    region: str | None = None
    min_lat: float | None = None
    max_lat: float | None = None
    min_lon: float | None = None
    max_lon: float | None = None
    weekly_average_trips: float
    weekly_breakdown: list[dict]


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    progress_percent: float
    total_rows: int | None = None
    processed_rows: int
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
