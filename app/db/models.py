from datetime import datetime
from uuid import uuid4

from sqlalchemy import Date, DateTime, Float, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"

    job_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    progress_percent: Mapped[float] = mapped_column(Numeric(5, 2), default=0)
    total_rows: Mapped[int | None] = mapped_column(Integer, nullable=True)
    processed_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TripClean(Base):
    __tablename__ = "trips_clean"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    city: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str | None] = mapped_column(String(255), nullable=True)
    origin_lat: Mapped[float] = mapped_column(Float, nullable=False)
    origin_lon: Mapped[float] = mapped_column(Float, nullable=False)
    destination_lat: Mapped[float] = mapped_column(Float, nullable=False)
    destination_lon: Mapped[float] = mapped_column(Float, nullable=False)
    origin_cell: Mapped[str] = mapped_column(String(64), nullable=False)
    destination_cell: Mapped[str] = mapped_column(String(64), nullable=False)
    trip_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    time_of_day: Mapped[str] = mapped_column(String(32), nullable=False)
    datasource: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    ingestion_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class TripGroupSummary(Base):
    __tablename__ = "trip_group_summary"
    __table_args__ = (
        UniqueConstraint(
            "city",
            "region",
            "origin_cell",
            "destination_cell",
            "time_of_day",
            name="uq_trip_group_summary_key",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    city: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str | None] = mapped_column(String(255), nullable=True)
    origin_cell: Mapped[str] = mapped_column(String(64), nullable=False)
    destination_cell: Mapped[str] = mapped_column(String(64), nullable=False)
    time_of_day: Mapped[str] = mapped_column(String(32), nullable=False)
    trip_count: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class WeeklyRegionStat(Base):
    __tablename__ = "weekly_region_stats"
    __table_args__ = (
        UniqueConstraint("city", "region", "week_start", name="uq_weekly_region_stats_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    city: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str | None] = mapped_column(String(255), nullable=True)
    week_start: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    trip_count: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
