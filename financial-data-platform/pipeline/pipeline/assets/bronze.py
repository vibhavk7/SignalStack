"""Bronze layer assets that ingest source rows with history-preserving hashes."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Final

import pandas as pd
from dagster import MetadataValue, asset
from sqlalchemy import bindparam, text

from connectors import OracleConnector, PostgresConnector
from pipeline.resources import OracleResource, PostgresResource
from pipeline.utils.hashing import compute_row_hash, filter_changed_rows, get_existing_hashes

LOGGER = logging.getLogger(__name__)

BRONZE_SCHEMA: Final[str] = "bronze"
PRIMARY_KEYS: Final[dict[str, str]] = {
    "customer_accounts": "account_id",
    "transactions": "transaction_id",
    "risk_flags": "flag_id",
    "monthly_summaries": "summary_id",
}


def _deactivate_current_rows(
    postgres: PostgresConnector,
    table_name: str,
    pk_col: str,
    keys: list[str],
) -> None:
    """Set previous current versions to false for changed source keys."""

    if not keys or not postgres.table_exists(table_name, BRONZE_SCHEMA):
        return
    statement = text(
        f"UPDATE {postgres.qualified_table_name(table_name, BRONZE_SCHEMA)} "
        f'SET "_is_current" = :new_value '
        f'WHERE "{pk_col}" IN :keys AND "_is_current" = :old_value'
    ).bindparams(bindparam("keys", expanding=True))
    with postgres.engine.begin() as conn:
        conn.execute(statement, {"new_value": False, "old_value": True, "keys": keys})


def materialize_bronze_table(
    source: OracleConnector,
    postgres: PostgresConnector,
    table_name: str,
    pk_col: str,
) -> dict[str, int | str]:
    """Ingest one source table into Bronze with row-level change detection."""

    source_df = source.read_table(table_name)
    if source_df.empty:
        LOGGER.warning("Source table %s returned no rows", table_name)
        return {"table": table_name, "source_rows": 0, "inserted_rows": 0}

    source_records = source_df.to_dict(orient="records")
    working = source_df.copy()
    working["_row_hash"] = [compute_row_hash(row) for row in source_records]

    existing_hashes = get_existing_hashes(postgres.engine, f"{BRONZE_SCHEMA}.{table_name}", pk_col)
    changed_df = filter_changed_rows(working, existing_hashes, pk_col)
    if changed_df.empty:
        return {"table": table_name, "source_rows": len(source_df), "inserted_rows": 0}

    changed_keys = changed_df[pk_col].astype(str).tolist()
    _deactivate_current_rows(postgres, table_name, pk_col, changed_keys)

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    changed_df["_ingested_at"] = now
    changed_df["_source_query"] = table_name
    changed_df["_is_current"] = True
    inserted_rows = postgres.write_dataframe(
        changed_df,
        table=table_name,
        schema=BRONZE_SCHEMA,
        if_exists="append",
    )
    return {"table": table_name, "source_rows": len(source_df), "inserted_rows": inserted_rows}


def _metadata(result: dict[str, int | str]) -> dict[str, MetadataValue]:
    return {
        "table": MetadataValue.text(str(result["table"])),
        "source_rows": MetadataValue.int(int(result["source_rows"])),
        "inserted_rows": MetadataValue.int(int(result["inserted_rows"])),
    }


@asset(group_name="bronze", compute_kind="python")
def bronze_customer_accounts(
    context,
    oracle: OracleResource,
    postgres: PostgresResource,
) -> Any:
    """Ingest customer account rows into Bronze."""

    result = materialize_bronze_table(
        source=oracle.get_connector(),
        postgres=postgres.get_connector(),
        table_name="customer_accounts",
        pk_col=PRIMARY_KEYS["customer_accounts"],
    )
    context.add_output_metadata(_metadata(result))
    return result


@asset(group_name="bronze", compute_kind="python")
def bronze_transactions(
    context,
    oracle: OracleResource,
    postgres: PostgresResource,
) -> Any:
    """Ingest transaction rows into Bronze."""

    result = materialize_bronze_table(
        source=oracle.get_connector(),
        postgres=postgres.get_connector(),
        table_name="transactions",
        pk_col=PRIMARY_KEYS["transactions"],
    )
    context.add_output_metadata(_metadata(result))
    return result


@asset(group_name="bronze", compute_kind="python")
def bronze_risk_flags(
    context,
    oracle: OracleResource,
    postgres: PostgresResource,
) -> Any:
    """Ingest risk flag rows into Bronze."""

    result = materialize_bronze_table(
        source=oracle.get_connector(),
        postgres=postgres.get_connector(),
        table_name="risk_flags",
        pk_col=PRIMARY_KEYS["risk_flags"],
    )
    context.add_output_metadata(_metadata(result))
    return result


@asset(group_name="bronze", compute_kind="python")
def bronze_monthly_summaries(
    context,
    oracle: OracleResource,
    postgres: PostgresResource,
) -> Any:
    """Ingest monthly summary rows into Bronze."""

    result = materialize_bronze_table(
        source=oracle.get_connector(),
        postgres=postgres.get_connector(),
        table_name="monthly_summaries",
        pk_col=PRIMARY_KEYS["monthly_summaries"],
    )
    context.add_output_metadata(_metadata(result))
    return result
