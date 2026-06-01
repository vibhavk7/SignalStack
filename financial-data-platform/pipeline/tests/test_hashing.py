"""Unit tests for row hashing helpers."""

from __future__ import annotations

import pandas as pd

from pipeline.utils.hashing import compute_row_hash, filter_changed_rows


def test_compute_row_hash_is_stable_and_ignores_excluded_columns() -> None:
    """Hashes should be deterministic regardless of dictionary insertion order."""

    row_a = {"account_id": "A1", "balance": 100.0, "_ingested_at": "later"}
    row_b = {"balance": 100.0, "_ingested_at": "earlier", "account_id": "A1"}

    assert compute_row_hash(row_a) == compute_row_hash(row_b)
    assert compute_row_hash(row_a) != compute_row_hash({"account_id": "A1", "balance": 200.0})
    assert compute_row_hash(row_a, exclude_cols=["balance"]) == compute_row_hash(
        {"account_id": "A1", "balance": 200.0},
        exclude_cols=["balance"],
    )


def test_filter_changed_rows_returns_new_and_modified_rows() -> None:
    """Rows with unknown keys or new hashes should pass through the filter."""

    df = pd.DataFrame(
        [
            {"id": "1", "value": "same"},
            {"id": "2", "value": "changed"},
            {"id": "3", "value": "new"},
        ]
    )
    df["_row_hash"] = [compute_row_hash(row) for row in df.drop(columns=["_row_hash"], errors="ignore").to_dict("records")]
    existing_hashes = {
        "1": df.loc[0, "_row_hash"],
        "2": compute_row_hash({"id": "2", "value": "old"}),
    }

    changed = filter_changed_rows(df, existing_hashes, "id")

    assert changed["id"].tolist() == ["2", "3"]
