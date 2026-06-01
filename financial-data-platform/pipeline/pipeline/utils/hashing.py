"""Row hashing helpers used for Bronze change detection."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


def _normalise_value(value: Any) -> Any:
    """Convert values to a deterministic JSON-friendly representation."""

    if pd.isna(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def compute_row_hash(row: dict[str, Any], exclude_cols: list[str] | None = None) -> str:
    """Compute a deterministic MD5 hash for all non-excluded row values.

    Assumption: source primary keys are stable and do not change.
    """

    excluded = set(exclude_cols or [])
    payload = {
        key: _normalise_value(row[key])
        for key in sorted(row)
        if key not in excluded and not key.startswith("_")
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.md5(encoded.encode("utf-8")).hexdigest()


def _split_table(table: str) -> tuple[str | None, str]:
    if "." not in table:
        return None, table
    schema, table_name = table.split(".", 1)
    return schema, table_name


def _physical_table(engine: Engine, table: str) -> tuple[str | None, str]:
    schema, table_name = _split_table(table)
    if engine.dialect.name == "sqlite" and schema:
        return None, f"{schema}__{table_name}"
    return schema, table_name


def _qualified_name(engine: Engine, table: str) -> str:
    schema, table_name = _physical_table(engine, table)
    if engine.dialect.name != "sqlite" and schema:
        return f'"{schema}"."{table_name}"'
    return f'"{table_name}"'


def _table_exists(engine: Engine, table: str) -> bool:
    schema, table_name = _physical_table(engine, table)
    inspector = inspect(engine)
    return inspector.has_table(table_name, schema=schema)


def get_existing_hashes(engine: Engine, table: str, pk_col: str) -> dict[str, str]:
    """Return current Bronze row hashes keyed by source primary key."""

    if not _table_exists(engine, table):
        return {}
    sql = (
        f'SELECT "{pk_col}" AS pk, "_row_hash" AS row_hash '
        f"FROM {_qualified_name(engine, table)} WHERE \"_is_current\" = "
        f"{'1' if engine.dialect.name == 'sqlite' else 'TRUE'}"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    return {str(row["pk"]): str(row["row_hash"]) for row in rows}


def filter_changed_rows(
    df: pd.DataFrame,
    existing_hashes: dict[str, str],
    pk_col: str,
) -> pd.DataFrame:
    """Return only rows whose source key is new or has a different row hash."""

    if df.empty:
        return df.copy()
    if pk_col not in df.columns:
        raise KeyError(f"Primary key column '{pk_col}' is missing from source dataframe")
    working = df.copy()
    if "_row_hash" not in working.columns:
        working["_row_hash"] = [
            compute_row_hash(row, exclude_cols=[]) for row in working.to_dict(orient="records")
        ]
    key_series = working[pk_col].astype(str)
    changed_mask = [
        existing_hashes.get(key) != row_hash
        for key, row_hash in zip(key_series, working["_row_hash"], strict=True)
    ]
    return working.loc[changed_mask].copy()
