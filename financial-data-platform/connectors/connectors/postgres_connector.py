"""Postgres connector implemented with SQLAlchemy 2.0 and pandas."""

from __future__ import annotations

import logging
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from connectors.base import BaseConnector
from connectors.config import PostgresSettings

LOGGER = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):
    """Read and write tabular data to Postgres schemas."""

    def __init__(self, settings: PostgresSettings | None = None, engine: Engine | None = None) -> None:
        self.settings = settings or PostgresSettings()
        self._engine = engine or create_engine(self.settings.sqlalchemy_url, future=True)

    @property
    def engine(self) -> Engine:
        """Return the SQLAlchemy engine."""

        return self._engine

    @property
    def supports_schemas(self) -> bool:
        """Return whether the current database dialect supports schemas."""

        return self.engine.dialect.name != "sqlite"

    def physical_table_name(self, table: str, schema: str | None = None) -> str:
        """Return the actual table name used by the current dialect."""

        if self.supports_schemas or schema is None:
            return table
        return f"{schema}__{table}"

    def qualified_table_name(self, table: str, schema: str | None = None) -> str:
        """Return a SQL-safe qualified table reference for controlled internal names."""

        physical = self.physical_table_name(table, schema)
        if self.supports_schemas and schema is not None:
            return f'"{schema}"."{table}"'
        return f'"{physical}"'

    def ensure_schema(self, schema: str) -> None:
        """Create a schema when the target dialect supports schemas."""

        if not self.supports_schemas:
            return
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    def read_sql(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        """Execute SQL and return a DataFrame."""

        LOGGER.info("Reading SQL from target database")
        with self.engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn, params=params)

    def read_table(
        self,
        table: str,
        schema: str | None = None,
        columns: Iterable[str] | None = None,
        where: str | None = None,
    ) -> pd.DataFrame:
        """Read a table from the target database."""

        if not self.table_exists(table, schema):
            return pd.DataFrame()
        column_sql = ", ".join(f'"{column}"' for column in columns) if columns else "*"
        sql = f"SELECT {column_sql} FROM {self.qualified_table_name(table, schema)}"
        if where:
            sql = f"{sql} WHERE {where}"
        return self.read_sql(sql)

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str | None = None,
        if_exists: str = "append",
        index: bool = False,
    ) -> int:
        """Write a DataFrame to a target table."""

        self.ensure_schema(schema) if schema else None
        physical_table = self.physical_table_name(table, schema)
        target_schema = schema if self.supports_schemas else None
        if df.empty and if_exists != "replace":
            LOGGER.info("Skipping empty append to %s.%s", schema, table)
            return 0
        LOGGER.info("Writing %s rows to %s.%s", len(df), schema, physical_table)
        df.to_sql(
            name=physical_table,
            con=self.engine,
            schema=target_schema,
            if_exists=if_exists,
            index=index,
            method="multi",
            chunksize=1000,
        )
        return int(len(df))

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        """Execute a write statement."""

        with self.engine.begin() as conn:
            conn.execute(text(sql), params or {})

    def table_exists(self, table: str, schema: str | None = None) -> bool:
        """Return whether a target table exists."""

        inspector = inspect(self.engine)
        if self.supports_schemas:
            return inspector.has_table(table, schema=schema)
        return inspector.has_table(self.physical_table_name(table, schema))

    def close(self) -> None:
        """Dispose of the engine."""

        self.engine.dispose()
