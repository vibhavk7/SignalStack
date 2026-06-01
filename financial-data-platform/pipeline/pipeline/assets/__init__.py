"""Dagster asset definitions for Bronze, Silver, and Gold layers."""

from pipeline.assets.bronze import (
    bronze_customer_accounts,
    bronze_monthly_summaries,
    bronze_risk_flags,
    bronze_transactions,
)
from pipeline.assets.gold import (
    analytics_monthly_check,
    gold_analytics_monthly,
    gold_risk_features,
    risk_features_check,
)
from pipeline.assets.silver import (
    silver_customer_accounts,
    silver_monthly_summaries,
    silver_risk_flags,
    silver_transactions,
)

ALL_ASSETS = [
    bronze_customer_accounts,
    bronze_transactions,
    bronze_risk_flags,
    bronze_monthly_summaries,
    silver_customer_accounts,
    silver_transactions,
    silver_risk_flags,
    silver_monthly_summaries,
    gold_risk_features,
    gold_analytics_monthly,
]

ALL_ASSET_CHECKS = [risk_features_check, analytics_monthly_check]
