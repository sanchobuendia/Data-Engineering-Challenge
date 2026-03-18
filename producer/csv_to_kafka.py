from __future__ import annotations

import argparse
import csv
import json
import logging
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
import psycopg

from app.core.config import settings
from app.services.utils import build_event_hash, coordinate_cell, derive_time_of_day

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [producer] %(message)s")
logger = logging.getLogger(__name__)


REQUIRED_COLUMNS = {
    "city",
    "region",
    "origin_lat",
    "origin_lon",
    "destination_lat",
    "destination_lon",
    "trip_datetime",
    "datasource",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish CSV trips to Kafka")
    parser.add_argument("--file", required=True, help="Path to the CSV file")
    parser.add_argument("--job-id", required=True, help="Ingestion job identifier")
    return parser.parse_args()


def count_rows(file_path: Path) -> int:
    with file_path.open("r", newline="", encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def update_job(conn: psycopg.Connection, job_id: str, **fields) -> None:
    set_clause = ", ".join(f"{key} = %s" for key in fields)
    values = list(fields.values()) + [job_id]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE ingestion_jobs SET {set_clause} WHERE job_id = %s", values)
    conn.commit()


def build_payload(row: dict, job_id: str) -> dict:
    trip_dt = datetime.fromisoformat(row["trip_datetime"])
    payload = {
        "city": row["city"],
        "region": row.get("region") or None,
        "origin_lat": float(row["origin_lat"]),
        "origin_lon": float(row["origin_lon"]),
        "destination_lat": float(row["destination_lat"]),
        "destination_lon": float(row["destination_lon"]),
        "trip_datetime": trip_dt.isoformat(),
        "time_of_day": derive_time_of_day(trip_dt),
        "origin_cell": coordinate_cell(float(row["origin_lat"]), float(row["origin_lon"])),
        "destination_cell": coordinate_cell(float(row["destination_lat"]), float(row["destination_lon"])),
        "datasource": row.get("datasource") or None,
        "ingestion_job_id": job_id,
    }
    payload["event_hash"] = build_event_hash(payload)
    return payload


def publish_csv(file_path: str, job_id: str) -> None:
    file_path = Path(file_path)
    total_rows = count_rows(file_path)

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    with psycopg.connect(settings.psycopg_database_uri) as conn:
        update_job(
            conn,
            job_id,
            total_rows=total_rows,
            processed_rows=0,
            progress_percent=0,
        )

        processed = 0
        try:
            with file_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                missing = REQUIRED_COLUMNS.difference(reader.fieldnames or [])
                if missing:
                    raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

                for row in reader:
                    payload = build_payload(row, job_id)
                    producer.send(settings.kafka_topic_raw, value=payload)
                    processed += 1
                    progress = round((processed / total_rows) * 100, 2) if total_rows else 100
                    update_job(
                        conn,
                        job_id,
                        processed_rows=processed,
                        total_rows=total_rows,
                        progress_percent=progress,
                    )

            producer.flush()
            update_job(
                conn,
                job_id,
                status="published_to_kafka",
                processed_rows=processed,
                total_rows=total_rows,
                progress_percent=100,
            )
            logger.info("Completed publishing CSV rows to Kafka", extra={"job_id": job_id})
        except Exception as exc:
            logger.exception("CSV producer failed")
            update_job(
                conn,
                job_id,
                status="failed",
                error_message=str(exc),
                finished_at=datetime.utcnow(),
            )
            raise
        finally:
            producer.close()


def main() -> None:
    args = parse_args()
    publish_csv(file_path=args.file, job_id=args.job_id)


if __name__ == "__main__":
    main()
