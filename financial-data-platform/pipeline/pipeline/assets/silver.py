"""Silver layer assets that clean, type, and deduplicate current Bronze rows."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Final

import pandas as pd
from dagster import MetadataValue, asset

from connectors import PostgresConnector
from pipeline.assets.bronze import BRONZE_SCHEMA, PRIMARY_KEYS
from pipeline.resources import PostgresResource

LOGGER = logging.getLogger(__name__)

SILVER_SCHEMA: Final[str] = "silver"
PIPELINE_VERSION: Final[str] = "1.0.0"

DATE_COLUMNS: Final[dict[str, list[str]]] = {
    "customer_accounts": ["open_date"],
    "transactions": ["transaction_date"],
    "risk_flags": ["flag_date", "resolved_date"],
    "monthly_summaries": [],
}
NUMERIC_COLUMNS: Final[dict[str, list[str]]] = {
    "customer_accounts": ["credit_limit", "balance"],
    "transactions": ["amount"],
    "risk_flags": [],
    "monthly_summaries": [
        "total_credits",
        "total_debits",
        "avg_balance",
        "transaction_count",
        "flagged_count",
    ],
}


def normalise_column_name(value: str) -> str:
    """Convert a column name to lower snake_case."""

    value = re.sub(r"[^0-9a-zA-Z]+", "_", value.strip())
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.lower().strip("_")


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    working.columns = [normalise_column_name(column) for column in working.columns]
    return working


def _handle_nulls(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    working = df.copy()
    for column in NUMERIC_COLUMNS.get(table_name, []):
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0)
    for column in DATE_COLUMNS.get(table_name, []):
        if column in working.columns:
            working[column] = pd.to_datetime(working[column], errors="coerce").dt.date
    for column in working.columns:
        if (
            column not in DATE_COLUMNS.get(table_name, [])
            and (
                pd.api.types.is_object_dtype(working[column])
                or pd.api.types.is_string_dtype(working[column])
            )
        ):
            working[column] = working[column].fillna("unknown")
    if "resolved" in working.columns:
        working["resolved"] = working["resolved"].fillna(False).astype(bool)
    return working


def process_silver_table(
    postgres: PostgresConnector,
    table_name: str,
    pk_col: str,
) -> pd.DataFrame:
    """Read current Bronze rows, clean them, and replace the Silver table."""

    bronze_df = postgres.read_table(
        table=table_name,
        schema=BRONZE_SCHEMA,
        where='"_is_current" = TRUE' if postgres.supports_schemas else '"_is_current" = 1',
    )
    if bronze_df.empty:
        LOGGER.warning("No current Bronze rows found for %s", table_name)
        return bronze_df

    silver_df = _normalise_columns(bronze_df)
    pk_col = normalise_column_name(pk_col)
    if "_ingested_at" in silver_df.columns:
        silver_df["_ingested_at"] = pd.to_datetime(silver_df["_ingested_at"], errors="coerce")
        silver_df = silver_df.sort_values("_ingested_at")
    silver_df = silver_df.drop_duplicates(subset=[pk_col], keep="last")
    silver_df = _handle_nulls(silver_df, table_name)
    silver_df["_pipeline_version"] = PIPELINE_VERSION
    silver_df["_processed_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    postgres.write_dataframe(silver_df, table=table_name, schema=SILVER_SCHEMA, if_exists="replace")
    return silver_df


def _metadata(table_name: str, df: pd.DataFrame) -> dict[str, MetadataValue]:
    return {"table": MetadataValue.text(table_name), "rows": MetadataValue.int(int(len(df)))}


@asset(group_name="silver", compute_kind="pandas")
def silver_customer_accounts(
    context,
    postgres: PostgresResource,
    bronze_customer_accounts: Any,
) -> Any:
    """Build the Silver customer accounts table."""

    del bronze_customer_accounts
    df = process_silver_table(postgres.get_connector(), "customer_accounts", PRIMARY_KEYS["customer_accounts"])
    context.add_output_metadata(_metadata("customer_accounts", df))
    return {"table": "customer_accounts", "rows": len(df)}


@asset(group_name="silver", compute_kind="pandas")
def silver_transactions(
    context,
    postgres: PostgresResource,
    bronze_transactions: Any,
) -> Any:
    """Build the Silver transactions table."""

    del bronze_transactions
    df = process_silver_table(postgres.get_connector(), "transactions", PRIMARY_KEYS["transactions"])
    context.add_output_metadata(_metadata("transactions", df))
    return {"table": "transactions", "rows": len(df)}


@asset(group_name="silver", compute_kind="pandas")
def silver_risk_flags(
    context,
    postgres: PostgresResource,
    bronze_risk_flags: Any,
) -> Any:
    """Build the Silver risk flags table."""

    del bronze_risk_flags
    df = process_silver_table(postgres.get_connector(), "risk_flags", PRIMARY_KEYS["risk_flags"])
    context.add_output_metadata(_metadata("risk_flags", df))
    return {"table": "risk_flags", "rows": len(df)}


@asset(group_name="silver", compute_kind="pandas")
def silver_monthly_summaries(
    context,
    postgres: PostgresResource,
    bronze_monthly_summaries: Any,
) -> Any:
    """Build the Silver monthly summaries table."""

    del bronze_monthly_summaries
    df = process_silver_table(
        postgres.get_connector(),
        "monthly_summaries",
        PRIMARY_KEYS["monthly_summaries"],
    )
    context.add_output_metadata(_metadata("monthly_summaries", df))
    return {"table": "monthly_summaries", "rows": len(df)}
