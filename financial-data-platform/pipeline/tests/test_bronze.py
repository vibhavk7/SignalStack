"""Tests for Bronze asset helper behavior."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

from connectors import OracleConnector, PostgresConnector
from pipeline.assets.bronze import materialize_bronze_table


def _source_accounts() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "account_id": "A1",
                "customer_id": "C1",
                "customer_name": "Grace Hopper",
                "account_type": "current",
                "open_date": "2024-01-01",
                "status": "active",
                "credit_limit": 1000.0,
                "balance": 100.0,
                "currency": "GBP",
                "branch_code": "BR001",
            },
            {
                "account_id": "A2",
                "customer_id": "C2",
                "customer_name": "Katherine Johnson",
                "account_type": "savings",
                "open_date": "2024-02-01",
                "status": "active",
                "credit_limit": 2000.0,
                "balance": 500.0,
                "currency": "GBP",
                "branch_code": "BR002",
            },
        ]
    )


def test_bronze_materialises_new_rows_and_versions_changed_rows() -> None:
    """Bronze should insert only new hashes and keep historical versions."""

    source_engine = create_engine("sqlite:///:memory:", future=True)
    target_engine = create_engine("sqlite:///:memory:", future=True)
    _source_accounts().to_sql("customer_accounts", source_engine, index=False)

    source = OracleConnector(engine=source_engine)
    target = PostgresConnector(engine=target_engine)

    first = materialize_bronze_table(source, target, "customer_accounts", "account_id")
    second = materialize_bronze_table(source, target, "customer_accounts", "account_id")
    with source_engine.begin() as conn:
        conn.execute(text("UPDATE customer_accounts SET balance = 175.0 WHERE account_id = 'A1'"))
    third = materialize_bronze_table(source, target, "customer_accounts", "account_id")

    result = target.read_table("customer_accounts", schema="bronze")
    current = result[result["_is_current"].astype(bool)]
    a1_versions = result[result["account_id"] == "A1"].sort_values("_ingested_at")

    assert first["inserted_rows"] == 2
    assert second["inserted_rows"] == 0
    assert third["inserted_rows"] == 1
    assert len(result) == 3
    assert len(current) == 2
    assert a1_versions["_is_current"].astype(bool).tolist() == [False, True]
