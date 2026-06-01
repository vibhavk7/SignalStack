"""Tests for Silver layer transformations."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

from connectors import PostgresConnector
from pipeline.assets.silver import process_silver_table


def test_silver_handles_nulls_deduplicates_and_normalises_columns() -> None:
    """Silver should keep current rows, normalise names, and fill sensible defaults."""

    engine = create_engine("sqlite:///:memory:", future=True)
    connector = PostgresConnector(engine=engine)
    bronze = pd.DataFrame(
        [
            {
                "account_id": "A1",
                "customer_id": "C1",
                "customer_name": None,
                "account_type": "current",
                "open_date": "2024-01-01",
                "status": None,
                "credit_limit": None,
                "balance": 100.0,
                "currency": "GBP",
                "branch_code": "BR001",
                "_ingested_at": datetime(2024, 1, 1, 1, 0, 0),
                "_row_hash": "old",
                "_source_query": "customer_accounts",
                "_is_current": False,
            },
            {
                "account_id": "A1",
                "customer_id": "C1",
                "customer_name": None,
                "account_type": "current",
                "open_date": "2024-01-02",
                "status": None,
                "credit_limit": None,
                "balance": None,
                "currency": "GBP",
                "branch_code": "BR001",
                "_ingested_at": datetime(2024, 1, 2, 1, 0, 0),
                "_row_hash": "new",
                "_source_query": "customer_accounts",
                "_is_current": True,
            },
        ]
    )
    connector.write_dataframe(bronze, "customer_accounts", schema="bronze", if_exists="replace")

    result = process_silver_table(connector, "customer_accounts", "account_id")

    assert len(result) == 1
    assert result.loc[0, "customer_name"] == "unknown"
    assert result.loc[0, "status"] == "unknown"
    assert result.loc[0, "credit_limit"] == 0
    assert result.loc[0, "balance"] == 0
    assert result.loc[0, "_pipeline_version"] == "1.0.0"
