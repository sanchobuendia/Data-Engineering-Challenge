# Data Engineering Challenge — Airflow + Kafka + PySpark + PostgreSQL

This repository implements the **stronger architecture** for the challenge: **Airflow for orchestration**, **Kafka for decoupled ingestion**, **PySpark Structured Streaming for scalable processing**, **PostgreSQL as the SQL serving layer**, and **FastAPI only for analytics and ingestion-status streaming**.

This design is intentionally closer to a real **Databricks-style data platform** than a simple REST ingestion endpoint, while still satisfying the challenge requirements: automated ingestion, grouping similar trips, weekly averages by region or bounding box, non-polling ingestion status, SQL persistence, and scalability considerations for very large datasets.

## Architecture

```text
CSV file dropped into shared volume / object storage
  ↓
Airflow DAG: trips_ingestion_pipeline
  ├── validate input
  ├── create ingestion job
  ├── publish CSV rows to Kafka topic: trips.raw
  └── wait for curated rows to land in PostgreSQL
  ↓
PySpark Structured Streaming consumer
  ↓
PostgreSQL
  ├── ingestion_jobs
  ├── trips_clean
  ├── trip_group_summary
  └── weekly_region_stats
  ↓
FastAPI
  ├── GET /grouped-trips
  ├── GET /weekly-average
  ├── GET /jobs/{job_id}
  └── WS  /ws/ingestion-status/{job_id}
```

## Why this design is stronger

A simpler batch-only Python API would also work, but this version maps much better to a modern data engineering stack:

- **Airflow** orchestrates the ingestion workflow and provides run history, retries, and operational visibility.
- **Kafka** decouples raw event ingestion from downstream processing.
- **PySpark Structured Streaming** provides scalable transformation and aggregation.
- **PostgreSQL** satisfies the SQL database requirement and serves curated analytics queries.
- **FastAPI** is limited to query-serving and asynchronous status delivery.
- **PostgreSQL LISTEN/NOTIFY + WebSocket** delivers ingestion updates without client polling.

## Trip similarity strategy

The challenge asks to group trips with similar origin, destination, and time of day.

To make “similar” operational and scalable, this project:

- rounds origin and destination coordinates into geographic cells (`origin_cell`, `destination_cell`)
- derives a `time_of_day` bucket from the trip timestamp
- groups by `(origin_cell, destination_cell, time_of_day, region, city)`

### Time-of-day buckets

- `night`: 00:00–05:59
- `morning`: 06:00–11:59
- `afternoon`: 12:00–17:59
- `evening`: 18:00–23:59

## Data model

### `ingestion_jobs`
Tracks the end-to-end state of each Airflow-triggered ingestion.

Possible statuses include:

- `pending`
- `running`
- `published_to_kafka`
- `processing`
- `completed`
- `failed`

### `trips_clean`
Stores validated and normalized trip events.

### `trip_group_summary`
Reserved curated aggregate table for grouped trips.

### `weekly_region_stats`
Reserved curated aggregate table for weekly aggregates.

## Project structure

```text
airflow/
  dags/
app/
  api/
  core/
  db/
  services/
producer/
  csv_to_kafka.py
scripts/
  job_utils.py
spark/
  common/
  jobs/
sql/
  schema.sql
  bonus_queries.sql
tests/
Dockerfile.airflow
Dockerfile.api
Dockerfile.spark
docker-compose.yml
```

## Running locally with Docker Compose

### 1. Start infrastructure

```bash
docker compose up --build
```

This starts:

- Zookeeper
- Kafka
- PostgreSQL
- Spark streaming consumer
- Airflow
- FastAPI analytics API

### 2. Access Airflow

Open:

```text
http://localhost:8080
```

The standalone image prints the generated admin password in the Airflow container logs.

### 3. Trigger the DAG

Trigger `trips_ingestion_pipeline` from the Airflow UI.

By default it ingests:

```text
/opt/airflow/data/sample_trips.csv
```

You can also trigger with custom DAG config:

```json
{
  "file_path": "/opt/airflow/data/sample_trips.csv"
}
```

### 4. Observe ingestion status without polling

Once the DAG creates a job, you can stream updates from:

```text
ws://localhost:8000/ws/ingestion-status/{job_id}
```

The API listens for PostgreSQL notifications and pushes status updates over WebSocket.

### 5. Query grouped trips

```bash
curl "http://localhost:8000/grouped-trips?city=New%20York"
```

### 6. Query weekly average by region

```bash
curl "http://localhost:8000/weekly-average?region=Brooklyn"
```

### 7. Query weekly average by bounding box

```bash
curl "http://localhost:8000/weekly-average?min_lat=40.70&max_lat=40.80&min_lon=-74.05&max_lon=-73.90"
```

## End-to-end ingestion flow

1. Airflow validates the file path.
2. Airflow creates a new row in `ingestion_jobs`.
3. Airflow publishes CSV rows to Kafka using the producer task.
4. Spark consumes `trips.raw` and writes curated rows into `trips_clean`.
5. Airflow waits until the expected number of curated rows exists in PostgreSQL.
6. Job completion is published through PostgreSQL notifications and exposed to clients via WebSocket.

## Expected CSV schema

The producer expects these columns:

- `city`
- `region`
- `origin_lat`
- `origin_lon`
- `destination_lat`
- `destination_lon`
- `trip_datetime`
- `datasource`

A sample file is included in `data/sample_trips.csv`.

## Scalability proof / design notes for 100M rows

This repository includes a locally runnable version, but the architecture is designed for scale:

- Airflow separates orchestration concerns from execution concerns.
- Kafka supports durable, decoupled ingestion.
- Spark Structured Streaming scales horizontally for transform and load.
- PostgreSQL serves curated analytics tables instead of acting as the sole raw lakehouse.
- data can be partitioned by event date in the curated layer.
- grouped and weekly tables reduce query cost for dashboards.
- indexes support region, time, and geo filters.
- the same logical design maps naturally to Databricks Bronze/Silver/Gold.

### Recommended production enhancements

- PostGIS for spatial indexing
- Delta tables for Bronze/Silver/Gold
- checkpointing on durable object storage
- Kafka Schema Registry
- dead-letter topics for invalid events
- separate OLTP and analytics-serving databases
- Airflow sensors/operators replaced with Databricks jobs or managed Spark jobs in production

## How this maps to Databricks

In a production Databricks setup, the Spark consumer in this repository would become a Structured Streaming job reading from Kafka and writing Bronze/Silver/Gold Delta tables. The weekly and grouped outputs could then be materialized into serving tables for BI or API access.

## Bonus SQL queries

See `sql/bonus_queries.sql`.

## Testing

```bash
pytest -q
```

## Note

This repository is a challenge-ready scaffold. It demonstrates the architecture, file layout, orchestration pattern, and core implementation choices you would explain in the interview. For production hardening, secrets management, retries, dead-letter handling, schema evolution, and richer observability should be expanded.
