"""Gold layer assets for customer risk features and monthly analytics."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from dagster import AssetCheckResult, MetadataValue
from dagster import asset, asset_check

from connectors import PostgresConnector
from pipeline.assets.silver import SILVER_SCHEMA
from pipeline.resources import PostgresResource

LOGGER = logging.getLogger(__name__)
GOLD_SCHEMA = "gold"
SEVERITY_SCORE = {"unknown": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}


def _read_silver(postgres: PostgresConnector, table_name: str) -> pd.DataFrame:
    return postgres.read_table(table=table_name, schema=SILVER_SCHEMA)


def _empty_risk_features() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "customer_id",
            "total_balance",
            "num_accounts",
            "days_since_last_transaction",
            "transaction_count_90d",
            "total_spend_90d",
            "num_risk_flags",
            "has_unresolved_flag",
            "max_flag_severity",
            "avg_monthly_debits",
            "credit_utilisation_ratio",
            "scoring_date",
        ]
    )


def build_risk_features(postgres: PostgresConnector) -> pd.DataFrame:
    """Build customer-level risk scoring features from Silver tables."""

    accounts = _read_silver(postgres, "customer_accounts")
    transactions = _read_silver(postgres, "transactions")
    risk_flags = _read_silver(postgres, "risk_flags")
    monthly = _read_silver(postgres, "monthly_summaries")
    if accounts.empty:
        result = _empty_risk_features()
        postgres.write_dataframe(result, "risk_features", GOLD_SCHEMA, if_exists="replace")
        return result

    today = pd.Timestamp(date.today())
    accounts["balance"] = pd.to_numeric(accounts["balance"], errors="coerce").fillna(0)
    accounts["credit_limit"] = pd.to_numeric(accounts["credit_limit"], errors="coerce").fillna(0)
    account_features = (
        accounts.groupby("customer_id", as_index=False)
        .agg(
            total_balance=("balance", "sum"),
            num_accounts=("account_id", "nunique"),
            total_credit_limit=("credit_limit", "sum"),
        )
    )
    account_features["credit_utilisation_ratio"] = (
        account_features["total_balance"] / account_features["total_credit_limit"].replace({0: pd.NA})
    ).fillna(0)

    if transactions.empty:
        transaction_features = pd.DataFrame({"customer_id": account_features["customer_id"]})
        transaction_features["days_since_last_transaction"] = 99999
        transaction_features["transaction_count_90d"] = 0
        transaction_features["total_spend_90d"] = 0.0
    else:
        tx = transactions.merge(accounts[["account_id", "customer_id"]], on="account_id", how="left")
        tx["transaction_date"] = pd.to_datetime(tx["transaction_date"], errors="coerce")
        tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce").fillna(0)
        cutoff = today - pd.Timedelta(days=90)
        recent = tx[tx["transaction_date"] >= cutoff].copy()
        last_tx = tx.groupby("customer_id", as_index=False)["transaction_date"].max()
        last_tx["days_since_last_transaction"] = (
            today - last_tx["transaction_date"].dt.normalize()
        ).dt.days.fillna(99999)
        counts = recent.groupby("customer_id", as_index=False).agg(
            transaction_count_90d=("transaction_id", "count"),
            total_spend_90d=("amount", lambda values: values[values < 0].abs().sum()),
        )
        transaction_features = last_tx[["customer_id", "days_since_last_transaction"]].merge(
            counts,
            on="customer_id",
            how="left",
        )

    if risk_flags.empty:
        flag_features = pd.DataFrame({"customer_id": account_features["customer_id"]})
        flag_features["num_risk_flags"] = 0
        flag_features["has_unresolved_flag"] = False
        flag_features["max_flag_severity"] = 0
    else:
        flags = risk_flags.copy()
        flags["severity_score"] = flags["severity"].map(SEVERITY_SCORE).fillna(0).astype(int)
        flag_features = flags.groupby("customer_id", as_index=False).agg(
            num_risk_flags=("flag_id", "count"),
            has_unresolved_flag=("resolved", lambda values: bool((~values.astype(bool)).any())),
            max_flag_severity=("severity_score", "max"),
        )

    if monthly.empty:
        debit_features = pd.DataFrame({"customer_id": account_features["customer_id"]})
        debit_features["avg_monthly_debits"] = 0.0
    else:
        monthly_joined = monthly.merge(accounts[["account_id", "customer_id"]], on="account_id", how="left")
        monthly_joined["total_debits"] = pd.to_numeric(
            monthly_joined["total_debits"], errors="coerce"
        ).fillna(0)
        debit_features = monthly_joined.groupby("customer_id", as_index=False).agg(
            avg_monthly_debits=("total_debits", "mean")
        )

    result = (
        account_features.drop(columns=["total_credit_limit"])
        .merge(transaction_features, on="customer_id", how="left")
        .merge(flag_features, on="customer_id", how="left")
        .merge(debit_features, on="customer_id", how="left")
    )
    defaults = {
        "days_since_last_transaction": 99999,
        "transaction_count_90d": 0,
        "total_spend_90d": 0.0,
        "num_risk_flags": 0,
        "has_unresolved_flag": False,
        "max_flag_severity": 0,
        "avg_monthly_debits": 0.0,
    }
    result = result.fillna(defaults)
    result["has_unresolved_flag"] = result["has_unresolved_flag"].astype(bool)
    result["scoring_date"] = date.today().isoformat()
    postgres.write_dataframe(result, "risk_features", GOLD_SCHEMA, if_exists="replace")
    return result


def build_analytics_monthly(postgres: PostgresConnector) -> pd.DataFrame:
    """Build monthly analytics from all Silver sources."""

    accounts = _read_silver(postgres, "customer_accounts")
    transactions = _read_silver(postgres, "transactions")
    risk_flags = _read_silver(postgres, "risk_flags")
    monthly = _read_silver(postgres, "monthly_summaries")
    month_values: set[str] = set(monthly["month_year"].astype(str)) if not monthly.empty else set()

    if not transactions.empty:
        transactions["transaction_date"] = pd.to_datetime(transactions["transaction_date"], errors="coerce")
        month_values.update(transactions["transaction_date"].dt.to_period("M").astype(str).dropna())
    if not accounts.empty:
        accounts["open_date"] = pd.to_datetime(accounts["open_date"], errors="coerce")
        month_values.update(accounts["open_date"].dt.to_period("M").astype(str).dropna())
    if not risk_flags.empty:
        risk_flags["flag_date"] = pd.to_datetime(risk_flags["flag_date"], errors="coerce")
        month_values.update(risk_flags["flag_date"].dt.to_period("M").astype(str).dropna())

    result = pd.DataFrame({"month_year": sorted(month_values)})
    if result.empty:
        postgres.write_dataframe(result, "analytics_monthly", GOLD_SCHEMA, if_exists="replace")
        return result

    if not accounts.empty:
        active_accounts = int((accounts["status"] == "active").sum()) if "status" in accounts else len(accounts)
        opened = (
            accounts.assign(month_year=accounts["open_date"].dt.to_period("M").astype(str))
            .groupby("month_year", as_index=False)
            .agg(new_accounts_opened=("account_id", "nunique"))
        )
        result["total_active_accounts"] = active_accounts
        result = result.merge(opened, on="month_year", how="left")
    else:
        result["total_active_accounts"] = 0
        result["new_accounts_opened"] = 0

    if not transactions.empty:
        tx = transactions.copy()
        tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce").fillna(0)
        tx["month_year"] = tx["transaction_date"].dt.to_period("M").astype(str)
        volume = tx.groupby("month_year", as_index=False).agg(
            total_transaction_volume=("amount", lambda values: values.abs().sum()),
            avg_transaction_value=("amount", lambda values: values.abs().mean()),
        )
        top_category = (
            tx.groupby("month_year")["merchant_category"]
            .agg(lambda values: values.mode().iat[0] if not values.mode().empty else "unknown")
            .reset_index(name="top_merchant_category")
        )
        result = result.merge(volume, on="month_year", how="left").merge(
            top_category, on="month_year", how="left"
        )
    else:
        result["total_transaction_volume"] = 0.0
        result["avg_transaction_value"] = 0.0
        result["top_merchant_category"] = "unknown"

    if not monthly.empty:
        monthly_numeric = monthly.copy()
        monthly_numeric["total_credits"] = pd.to_numeric(
            monthly_numeric["total_credits"], errors="coerce"
        ).fillna(0)
        monthly_numeric["total_debits"] = pd.to_numeric(
            monthly_numeric["total_debits"], errors="coerce"
        ).fillna(0)
        credits = monthly_numeric.groupby("month_year", as_index=False).agg(
            total_credits=("total_credits", "sum"),
            total_debits=("total_debits", "sum"),
        )
        result = result.merge(credits, on="month_year", how="left")
        if not accounts.empty:
            utilisation = monthly_numeric.merge(
                accounts[["account_id", "credit_limit"]],
                on="account_id",
                how="left",
            )
            utilisation["avg_balance"] = pd.to_numeric(utilisation["avg_balance"], errors="coerce").fillna(0)
            utilisation["credit_limit"] = pd.to_numeric(
                utilisation["credit_limit"], errors="coerce"
            ).fillna(0)
            util = utilisation.groupby("month_year", as_index=False).apply(
                lambda frame: pd.Series(
                    {
                        "avg_credit_utilisation": (
                            frame["avg_balance"].sum() / frame["credit_limit"].replace({0: pd.NA}).sum()
                        )
                        if frame["credit_limit"].sum() else 0
                    }
                ),
                include_groups=False,
            )
            result = result.merge(util, on="month_year", how="left")
    else:
        result["total_credits"] = 0.0
        result["total_debits"] = 0.0
        result["avg_credit_utilisation"] = 0.0

    if not risk_flags.empty:
        flags = risk_flags.copy()
        flags["month_year"] = flags["flag_date"].dt.to_period("M").astype(str)
        flag_counts = flags.groupby("month_year", as_index=False).agg(
            flagged_customers_count=("customer_id", "nunique")
        )
        result = result.merge(flag_counts, on="month_year", how="left")
    else:
        result["flagged_customers_count"] = 0

    result = result.fillna(
        {
            "new_accounts_opened": 0,
            "total_transaction_volume": 0.0,
            "avg_transaction_value": 0.0,
            "total_credits": 0.0,
            "total_debits": 0.0,
            "flagged_customers_count": 0,
            "avg_credit_utilisation": 0.0,
            "top_merchant_category": "unknown",
        }
    )
    postgres.write_dataframe(result, "analytics_monthly", GOLD_SCHEMA, if_exists="replace")
    return result


@asset(group_name="gold", compute_kind="pandas")
def gold_risk_features(
    context,
    postgres: PostgresResource,
    silver_customer_accounts: Any,
    silver_transactions: Any,
    silver_risk_flags: Any,
    silver_monthly_summaries: Any,
) -> Any:
    """Materialize the customer risk scoring feature table."""

    del silver_customer_accounts, silver_transactions, silver_risk_flags, silver_monthly_summaries
    df = build_risk_features(postgres.get_connector())
    context.add_output_metadata({"rows": MetadataValue.int(int(len(df)))})
    return {"table": "risk_features", "rows": len(df)}


@asset(group_name="gold", compute_kind="pandas")
def gold_analytics_monthly(
    context,
    postgres: PostgresResource,
    silver_customer_accounts: Any,
    silver_transactions: Any,
    silver_risk_flags: Any,
    silver_monthly_summaries: Any,
) -> Any:
    """Materialize the monthly analytics table."""

    del silver_customer_accounts, silver_transactions, silver_risk_flags, silver_monthly_summaries
    df = build_analytics_monthly(postgres.get_connector())
    context.add_output_metadata({"rows": MetadataValue.int(int(len(df)))})
    return {"table": "analytics_monthly", "rows": len(df)}


@asset_check(asset=gold_risk_features, name="required_fields_not_null")
def risk_features_check(
    context,
    postgres: PostgresResource,
) -> AssetCheckResult:
    """Assert required risk feature fields are populated."""

    del context
    df = postgres.get_connector().read_table("risk_features", GOLD_SCHEMA)
    passed = not df.empty and df[["customer_id", "scoring_date"]].notna().all().all()
    return AssetCheckResult(
        passed=bool(passed),
        metadata={"rows": int(len(df)), "checked_at": datetime.now(timezone.utc).isoformat()},
    )


@asset_check(asset=gold_analytics_monthly, name="monthly_rows_valid")
def analytics_monthly_check(
    context,
    postgres: PostgresResource,
) -> AssetCheckResult:
    """Assert analytics rows exist and month values use YYYY-MM format."""

    del context
    df = postgres.get_connector().read_table("analytics_monthly", GOLD_SCHEMA)
    valid_months = (
        not df.empty
        and df["month_year"].astype(str).str.match(r"^\d{4}-\d{2}$").all()
    )
    return AssetCheckResult(
        passed=bool(valid_months),
        metadata={"rows": int(len(df)), "checked_at": datetime.now(timezone.utc).isoformat()},
    )
