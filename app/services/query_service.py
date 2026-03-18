from __future__ import annotations

from collections.abc import Sequence
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.db.models import IngestionJob, TripClean


class QueryService:
    def __init__(self, db: Session):
        self.db = db

    def get_grouped_trips(self, city: str | None = None, region: str | None = None):
        stmt = (
            select(
                TripClean.city,
                TripClean.region,
                TripClean.origin_cell,
                TripClean.destination_cell,
                TripClean.time_of_day,
                func.count().label("trip_count"),
            )
            .group_by(
                TripClean.city,
                TripClean.region,
                TripClean.origin_cell,
                TripClean.destination_cell,
                TripClean.time_of_day,
            )
            .order_by(desc("trip_count"))
        )
        if city:
            stmt = stmt.where(TripClean.city == city)
        if region:
            stmt = stmt.where(TripClean.region == region)
        return self.db.execute(stmt.limit(100)).all()

    def get_weekly_average_by_region(self, region: str) -> tuple[float, list[dict]]:
        stmt = (
            select(
                func.date_trunc('week', TripClean.trip_datetime).label('week_start'),
                func.count().label('trip_count'),
            )
            .where(TripClean.region == region)
            .group_by(func.date_trunc('week', TripClean.trip_datetime))
            .order_by(func.date_trunc('week', TripClean.trip_datetime))
        )
        rows = self.db.execute(stmt).all()
        if not rows:
            return 0.0, []
        counts = [int(row.trip_count) for row in rows]
        avg = sum(counts) / len(counts)
        breakdown = [
            {"week_start": row.week_start.date().isoformat(), "trip_count": int(row.trip_count)} for row in rows
        ]
        return avg, breakdown

    def get_weekly_average_by_bounding_box(
        self,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
    ) -> tuple[float, list[dict]]:
        stmt = (
            select(
                func.date_trunc('week', TripClean.trip_datetime).label('week_start'),
                func.count().label('trip_count'),
            )
            .where(TripClean.origin_lat.between(min_lat, max_lat))
            .where(TripClean.origin_lon.between(min_lon, max_lon))
            .group_by(func.date_trunc('week', TripClean.trip_datetime))
            .order_by(func.date_trunc('week', TripClean.trip_datetime))
        )
        rows = self.db.execute(stmt).all()
        if not rows:
            return 0.0, []
        counts = [int(row.trip_count) for row in rows]
        avg = sum(counts) / len(counts)
        breakdown = [
            {"week_start": row.week_start.date().isoformat(), "trip_count": int(row.trip_count)} for row in rows
        ]
        return avg, breakdown

    def get_job(self, job_id: str) -> IngestionJob | None:
        return self.db.get(IngestionJob, job_id)
