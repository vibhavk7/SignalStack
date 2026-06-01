"""Pydantic settings models for source and target database connections."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    """Connection settings for the Postgres target database."""

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    database: str = Field(default="financial_platform")
    username: str = Field(default="postgres")
    password: str = Field(default="postgres")

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", extra="ignore")

    @property
    def sqlalchemy_url(self) -> str:
        """Build a SQLAlchemy URL for psycopg2."""

        return (
            f"postgresql+psycopg2://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class OracleSettings(BaseSettings):
    """Oracle-style connection settings with a local SQLite fallback."""

    host: str = Field(default="localhost")
    port: int = Field(default=1521)
    service_name: str = Field(default="ORCLCDB")
    username: str = Field(default="client_a_user")
    password: str = Field(default="oracle")
    sqlite_path: Path = Field(default=Path("seed_data/financial_data.sqlite"))

    model_config = SettingsConfigDict(env_prefix="ORACLE_", extra="ignore")

    @property
    def dsn(self) -> str:
        """Return the Oracle DSN string that production implementations can use."""

        return f"{self.host}:{self.port}/{self.service_name}"

    @property
    def sqlite_url(self) -> str:
        """Return a SQLAlchemy URL for the local SQLite source."""

        return f"sqlite:///{self.sqlite_path}"
