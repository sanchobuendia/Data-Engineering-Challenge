from __future__ import annotations

from datetime import datetime
from sqlalchemy.orm import Session

from app.db.models import IngestionJob


class JobService:
    def __init__(self, db: Session):
        self.db = db

    def create_job(self) -> IngestionJob:
        job = IngestionJob(status="pending", progress_percent=0, processed_rows=0)
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        return job

    def mark_running(self, job_id: str, total_rows: int | None = None) -> None:
        job = self.db.get(IngestionJob, job_id)
        if not job:
            return
        job.status = "running"
        job.started_at = datetime.utcnow()
        if total_rows is not None:
            job.total_rows = total_rows
        self.db.commit()

    def update_progress(self, job_id: str, processed_rows: int, total_rows: int | None = None) -> None:
        job = self.db.get(IngestionJob, job_id)
        if not job:
            return
        job.processed_rows = processed_rows
        if total_rows is not None:
            job.total_rows = total_rows
        if job.total_rows and job.total_rows > 0:
            job.progress_percent = round((processed_rows / job.total_rows) * 100, 2)
        self.db.commit()

    def mark_completed(self, job_id: str) -> None:
        job = self.db.get(IngestionJob, job_id)
        if not job:
            return
        job.status = "completed"
        job.progress_percent = 100
        job.finished_at = datetime.utcnow()
        self.db.commit()

    def mark_failed(self, job_id: str, error_message: str) -> None:
        job = self.db.get(IngestionJob, job_id)
        if not job:
            return
        job.status = "failed"
        job.error_message = error_message
        job.finished_at = datetime.utcnow()
        self.db.commit()
