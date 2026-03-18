from __future__ import annotations

import json
from datetime import datetime, date
from decimal import Decimal

import psycopg

from app.core.config import settings


STATUS_CHANNEL = "ingestion_job_status"


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def notify_job_event(payload: dict) -> None:
    message = json.dumps(payload, default=_json_default)
    with psycopg.connect(settings.psycopg_database_uri) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_notify(%s, %s)", (STATUS_CHANNEL, message))
        conn.commit()
