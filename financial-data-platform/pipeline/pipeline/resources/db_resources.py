"""Dagster resources that wrap reusable database connectors."""

from __future__ import annotations

from pathlib import Path

from dagster import ConfigurableResource
from pydantic import Field

from connectors import OracleConnector, OracleSettings, PostgresConnector, PostgresSettings


class PostgresResource(ConfigurableResource):
    """Configurable Dagster resource for the Postgres target."""

    host: str = "localhost"
    port: int = 5432
    database: str = "financial_platform"
    username: str = "postgres"
    password: str = "postgres"

    def get_connector(self) -> PostgresConnector:
        """Create a PostgresConnector from resource config."""

        return PostgresConnector(
            settings=PostgresSettings(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
            )
        )


class OracleResource(ConfigurableResource):
    """Configurable Dagster resource for the Oracle-compatible source."""

    host: str = "localhost"
    port: int = 1521
    service_name: str = "ORCLCDB"
    username: str = "client_a_user"
    password: str = "oracle"
    sqlite_path: str = Field(default="seed_data/financial_data.sqlite")

    def get_connector(self) -> OracleConnector:
        """Create an OracleConnector from resource config."""

        return OracleConnector(
            settings=OracleSettings(
                host=self.host,
                port=self.port,
                service_name=self.service_name,
                username=self.username,
                password=self.password,
                sqlite_path=Path(self.sqlite_path),
            )
        )
