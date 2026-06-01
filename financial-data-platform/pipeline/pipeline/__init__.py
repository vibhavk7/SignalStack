"""Dagster definitions for the financial data platform."""

from __future__ import annotations

import os

import pipeline.project_env  # noqa: F401  # load monorepo `.env` before config reads

from dagster import Definitions

from pipeline.assets import ALL_ASSET_CHECKS, ALL_ASSETS
from pipeline.resources import OracleResource, PostgresResource
from pipeline.schedules import daily_full_pipeline_schedule, full_pipeline_job
from pipeline.sensors import ChangeDetectionSensor

defs = Definitions(
    assets=ALL_ASSETS,
    asset_checks=ALL_ASSET_CHECKS,
    jobs=[full_pipeline_job],
    schedules=[daily_full_pipeline_schedule],
    sensors=[ChangeDetectionSensor],
    resources={
        "postgres": PostgresResource(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE", "financial_platform"),
            username=os.getenv("POSTGRES_USERNAME", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        ),
        "oracle": OracleResource(
            host=os.getenv("ORACLE_HOST", "localhost"),
            port=int(os.getenv("ORACLE_PORT", "1521")),
            service_name=os.getenv("ORACLE_SERVICE_NAME", "ORCLCDB"),
            username=os.getenv("ORACLE_USERNAME", "client_a_user"),
            password=os.getenv("CLIENT_A_ORACLE_PASSWORD", "oracle"),
            sqlite_path=os.getenv("SOURCE_SQLITE_PATH", "seed_data/financial_data.sqlite"),
        ),
    },
)
