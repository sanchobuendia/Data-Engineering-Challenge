from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from producer.csv_to_kafka import publish_csv
from scripts.job_utils import create_ingestion_job, update_job, wait_for_ingestion_completion

DEFAULT_FILE_PATH = "/opt/airflow/data/sample_trips.csv"


@dag(
    dag_id="trips_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["challenge", "kafka", "spark", "postgres"],
)
def trips_ingestion_pipeline():
    @task
    def create_job() -> str:
        return create_ingestion_job()

    @task
    def validate_input_file(dag_run=None) -> str:
        conf = dag_run.conf if dag_run else {}
        file_path = conf.get("file_path", DEFAULT_FILE_PATH)
        if not Path(file_path).exists():
            raise AirflowFailException(f"CSV file not found: {file_path}")
        return file_path

    @task
    def publish_events(file_path: str, job_id: str) -> str:
        update_job(job_id, status="running", started_at=datetime.utcnow())
        publish_csv(file_path=file_path, job_id=job_id)
        return job_id

    @task
    def wait_for_load(job_id: str) -> dict:
        return wait_for_ingestion_completion(job_id, timeout_seconds=600, poll_seconds=5)

    file_path = validate_input_file()
    job_id = create_job()
    published_job_id = publish_events(file_path, job_id)
    wait_for_load(published_job_id)


trips_ingestion_pipeline()
