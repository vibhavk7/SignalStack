"""Tests for connector read/write behavior."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine

from connectors import OracleConnector, PostgresConnector


def test_postgres_connector_reads_and_writes_using_sqlite_test_engine() -> None:
    """The target connector should support schema-like writes in SQLite tests."""

    engine = create_engine("sqlite:///:memory:", future=True)
    connector = PostgresConnector(engine=engine)
    df = pd.DataFrame([{"id": "1", "amount": 12.5}, {"id": "2", "amount": 9.0}])

    written = connector.write_dataframe(df, table="example", schema="silver", if_exists="replace")
    result = connector.read_table("example", schema="silver")

    assert written == 2
    assert result.sort_values("id")["amount"].tolist() == [12.5, 9.0]


def test_oracle_connector_reads_named_sqlite_result_set() -> None:
    """The source connector should read a configured query result by name."""

    engine = create_engine("sqlite:///:memory:", future=True)
    source = pd.DataFrame(
        [
            {
                "account_id": "A1",
                "customer_id": "C1",
                "customer_name": "Ada Lovelace",
                "account_type": "current",
                "open_date": "2024-01-01",
                "status": "active",
                "credit_limit": 1000.0,
                "balance": 100.0,
                "currency": "GBP",
                "branch_code": "BR001",
            }
        ]
    )
    source.to_sql("customer_accounts", engine, index=False)
    connector = OracleConnector(engine=engine)

    result = connector.read_table("customer_accounts")

    assert result.loc[0, "account_id"] == "A1"
