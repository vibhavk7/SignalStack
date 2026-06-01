"""Oracle-style connector backed by SQLite for local development."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from connectors.base import BaseConnector
from connectors.config import OracleSettings

LOGGER = logging.getLogger(__name__)

QUERY_MAP: dict[str, str] = {
    "customer_accounts": "SELECT * FROM customer_accounts",
    "transactions": "SELECT * FROM transactions",
    "risk_flags": "SELECT * FROM risk_flags",
    "monthly_summaries": "SELECT * FROM monthly_summaries",
}


class OracleConnector(BaseConnector):
    """Read-only connector that mimics Oracle result sets using SQLite locally."""

    def __init__(self, settings: OracleSettings | None = None, engine: Engine | None = None) -> None:
        self.settings = settings or OracleSettings()
        self._engine = engine or create_engine(self.settings.sqlite_url, future=True)

    @classmethod
    def from_sqlite_path(cls, sqlite_path: str | Path) -> "OracleConnector":
        """Create a connector from a local SQLite path."""

        settings = OracleSettings(sqlite_path=Path(sqlite_path))
        return cls(settings=settings)

    @property
    def engine(self) -> Engine:
        """Return the SQLAlchemy engine."""

        return self._engine

    def read_sql(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        """Execute a SQL query against the local source."""

        LOGGER.info("Reading Oracle-compatible SQL result set")
        with self.engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn, params=params)

    def read_query_result(self, query_name: str) -> pd.DataFrame:
        """Read one of the named client query result sets."""

        if query_name not in QUERY_MAP:
            raise KeyError(f"Unknown source query '{query_name}'. Expected one of {sorted(QUERY_MAP)}")
        return self.read_sql(QUERY_MAP[query_name])

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read a source table by name."""

        return self.read_query_result(table_name)

    def close(self) -> None:
        """Dispose of the engine."""

        self.engine.dispose()
