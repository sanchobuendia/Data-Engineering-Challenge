from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def launch_csv_producer(csv_path: str, job_id: str) -> None:
    command = [
        sys.executable,
        "-m",
        "producer.csv_to_kafka",
        "--file",
        csv_path,
        "--job-id",
        job_id,
    ]
    logger.info("Launching CSV producer", extra={"job_id": job_id, "csv_path": csv_path})
    subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
