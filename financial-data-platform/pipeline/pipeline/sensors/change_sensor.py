"""Sensor that checks source row counts and recent-row checksums for changes."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from pipeline.assets.bronze import PRIMARY_KEYS
from pipeline.schedules import full_pipeline_job

LOGGER = logging.getLogger(__name__)


def _sqlite_path() -> Path:
    return Path(os.getenv("SOURCE_SQLITE_PATH", "seed_data/financial_data.sqlite"))


def _table_state(conn: sqlite3.Connection, table_name: str, pk_col: str) -> dict[str, Any]:
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM {table_name} ORDER BY {pk_col} DESC LIMIT 100"
    ).fetchall()
    columns = [description[0] for description in conn.execute(f"SELECT * FROM {table_name} LIMIT 1").description]
    serialisable_rows = [dict(zip(columns, row, strict=True)) for row in rows]
    checksum = hashlib.md5(
        json.dumps(serialisable_rows, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return {"row_count": count, "recent_checksum": checksum}


def source_change_state(sqlite_path: Path) -> dict[str, dict[str, Any]]:
    """Return count and checksum state for all source result sets."""

    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite source database not found: {sqlite_path}")
    with sqlite3.connect(sqlite_path) as conn:
        return {
            table_name: _table_state(conn, table_name, pk_col)
            for table_name, pk_col in PRIMARY_KEYS.items()
        }


@sensor(job=full_pipeline_job, minimum_interval_seconds=900, name="ChangeDetectionSensor")
def ChangeDetectionSensor(context: SensorEvaluationContext) -> RunRequest | SkipReason:
    """Trigger the full asset job only when the SQLite source appears changed."""

    try:
        current_state = source_change_state(_sqlite_path())
    except FileNotFoundError as exc:
        LOGGER.warning("Source change sensor skipped: %s", exc)
        return SkipReason(str(exc))

    previous_state = json.loads(context.cursor) if context.cursor else None
    encoded_state = json.dumps(current_state, sort_keys=True)
    if previous_state == current_state:
        return SkipReason("No source row-count or recent-checksum changes detected.")

    context.update_cursor(encoded_state)
    run_key = hashlib.md5(encoded_state.encode("utf-8")).hexdigest()
    return RunRequest(run_key=run_key, run_config={})
