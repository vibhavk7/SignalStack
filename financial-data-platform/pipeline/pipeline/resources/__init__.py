"""Dagster resources used by pipeline assets."""

from pipeline.resources.db_resources import OracleResource, PostgresResource

__all__ = ["OracleResource", "PostgresResource"]
