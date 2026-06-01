"""Abstract base classes shared by database connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine


class BaseConnector(ABC):
    """Minimal interface implemented by all database connectors."""

    @property
    @abstractmethod
    def engine(self) -> Engine:
        """Return the SQLAlchemy engine owned by the connector."""

    @abstractmethod
    def read_sql(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        """Execute SQL and return a pandas DataFrame."""

    @abstractmethod
    def close(self) -> None:
        """Dispose of underlying database resources."""
