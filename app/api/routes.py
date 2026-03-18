from __future__ import annotations

import asyncio
import json
from pathlib import Path

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from app.api.schemas import GroupedTripResponse, JobStatusResponse, WeeklyAverageResponse
from app.db.session import get_db
from app.services.query_service import QueryService
from app.services.websocket_manager import manager
from app.db.session import SessionLocal

router = APIRouter()
@router.get("/health")
def healthcheck() -> dict:
    return {"status": "ok"}


@router.get("/grouped-trips", response_model=list[GroupedTripResponse])
def grouped_trips(
    city: str | None = None,
    region: str | None = None,
    db: Session = Depends(get_db),
) -> list[GroupedTripResponse]:
    service = QueryService(db)
    rows = service.get_grouped_trips(city=city, region=region)
    return [
        GroupedTripResponse(
            city=row.city,
            region=row.region,
            origin_cell=row.origin_cell,
            destination_cell=row.destination_cell,
            time_of_day=row.time_of_day,
            trip_count=row.trip_count,
        )
        for row in rows
    ]


@router.get("/weekly-average", response_model=WeeklyAverageResponse)
def weekly_average(
    region: str | None = None,
    min_lat: float | None = Query(default=None),
    max_lat: float | None = Query(default=None),
    min_lon: float | None = Query(default=None),
    max_lon: float | None = Query(default=None),
    db: Session = Depends(get_db),
) -> WeeklyAverageResponse:
    service = QueryService(db)

    if region:
        avg, breakdown = service.get_weekly_average_by_region(region)
        return WeeklyAverageResponse(
            filter_type="region",
            region=region,
            weekly_average_trips=avg,
            weekly_breakdown=breakdown,
        )

    coords = [min_lat, max_lat, min_lon, max_lon]
    if all(value is not None for value in coords):
        avg, breakdown = service.get_weekly_average_by_bounding_box(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        )
        return WeeklyAverageResponse(
            filter_type="bounding_box",
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            weekly_average_trips=avg,
            weekly_breakdown=breakdown,
        )

    raise HTTPException(
        status_code=400,
        detail="Provide either region or a complete bounding box (min_lat, max_lat, min_lon, max_lon)",
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def job_status(job_id: str, db: Session = Depends(get_db)) -> JobStatusResponse:
    service = QueryService(db)
    job = service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatusResponse(
        job_id=job.job_id,
        status=job.status,
        progress_percent=float(job.progress_percent),
        total_rows=job.total_rows,
        processed_rows=job.processed_rows,
        error_message=job.error_message,
        started_at=job.started_at,
        finished_at=job.finished_at,
    )


@router.websocket("/ws/ingestion-status/{job_id}")
async def ingestion_status(job_id: str, websocket: WebSocket) -> None:
    await manager.connect(job_id, websocket)
    db = SessionLocal()
    try:
        service = QueryService(db)
        job = service.get_job(job_id)
        if not job:
            await websocket.send_json({"error": "job not found", "job_id": job_id})
            await websocket.close(code=4404)
            return

        await websocket.send_json({
            "job_id": job.job_id,
            "status": job.status,
            "progress_percent": float(job.progress_percent),
            "total_rows": job.total_rows,
            "processed_rows": job.processed_rows,
            "error_message": job.error_message,
        })

        def wait_for_notification() -> str | None:
            with psycopg.connect(service.db.bind.url.render_as_string(hide_password=False), autocommit=True) as conn:  # type: ignore[arg-type]
                conn.execute("LISTEN ingestion_job_status;")
                while True:
                    notify = conn.notifies(timeout=30)
                    if notify is None:
                        return None
                    payload = json.loads(notify.payload)
                    if payload.get("job_id") == job_id:
                        return notify.payload

        while True:
            payload_str = await asyncio.to_thread(wait_for_notification)
            if payload_str is None:
                continue
            payload = json.loads(payload_str)
            await websocket.send_json(payload)
            if payload.get("status") in {"completed", "failed"}:
                break
    except WebSocketDisconnect:
        manager.disconnect(job_id, websocket)
    finally:
        db.close()
        manager.disconnect(job_id, websocket)
