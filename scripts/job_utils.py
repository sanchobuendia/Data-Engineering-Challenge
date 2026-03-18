from __future__ import annotations

import json
from datetime import datetime

import psycopg

from app.core.config import settings
from app.services.status_events import notify_job_event


def create_ingestion_job() -> str:
    with psycopg.connect(settings.psycopg_database_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_jobs (status, progress_percent, processed_rows)
                VALUES ('pending', 0, 0)
                RETURNING job_id
                """
            )
            job_id = cur.fetchone()[0]
        conn.commit()

    notify_job_event({
        "job_id": job_id,
        "status": "pending",
        "progress_percent": 0,
        "processed_rows": 0,
        "event": "job_created",
    })
    return job_id


def update_job(job_id: str, **fields) -> None:
    set_clause = ", ".join(f"{key} = %s" for key in fields)
    values = list(fields.values()) + [job_id]
    with psycopg.connect(settings.psycopg_database_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE ingestion_jobs SET {set_clause} WHERE job_id = %s", values)
            cur.execute(
                """
                SELECT job_id, status, progress_percent, total_rows, processed_rows,
                       error_message, started_at, finished_at
                FROM ingestion_jobs
                WHERE job_id = %s
                """,
                (job_id,),
            )
            row = cur.fetchone()
        conn.commit()

    if row:
        notify_job_event(
            {
                "job_id": row[0],
                "status": row[1],
                "progress_percent": float(row[2] or 0),
                "total_rows": row[3],
                "processed_rows": row[4],
                "error_message": row[5],
                "started_at": row[6],
                "finished_at": row[7],
            }
        )


def wait_for_ingestion_completion(job_id: str, timeout_seconds: int = 300, poll_seconds: int = 5) -> dict:
    import time

    deadline = time.time() + timeout_seconds
    last_count = 0
    while time.time() < deadline:
        with psycopg.connect(settings.psycopg_database_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT total_rows, processed_rows, status FROM ingestion_jobs WHERE job_id = %s",
                    (job_id,),
                )
                total_rows, processed_rows, status = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) FROM trips_clean WHERE ingestion_job_id = %s",
                    (job_id,),
                )
                loaded = cur.fetchone()[0]
            conn.commit()

        total_rows = total_rows or 0
        progress = round((loaded / total_rows) * 100, 2) if total_rows else 0
        if loaded != last_count or status != "processing":
            update_job(
                job_id,
                status="processing",
                processed_rows=loaded,
                progress_percent=progress,
            )
            last_count = loaded

        if total_rows and loaded >= total_rows:
            update_job(
                job_id,
                status="completed",
                processed_rows=loaded,
                progress_percent=100,
                finished_at=datetime.utcnow(),
            )
            return {"job_id": job_id, "loaded_rows": loaded, "status": "completed"}

        time.sleep(poll_seconds)

    update_job(
        job_id,
        status="failed",
        error_message=f"Timed out waiting for Spark load after {timeout_seconds} seconds",
        finished_at=datetime.utcnow(),
    )
    raise TimeoutError(f"Timed out waiting for ingestion job {job_id}")


if __name__ == "__main__":
    print(json.dumps({"job_id": create_ingestion_job()}))
