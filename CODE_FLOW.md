# How The App Works - Full Code Flow

This document describes how data moves through the Financial Data Platform and which files implement each step. Paths are relative to `financial-data-platform/` unless noted otherwise.

## Big Picture

```text
SQLite fake Oracle -> Bronze raw history -> Silver clean current -> Gold serving tables
                                                        |             |
                                                        |             +-> gold.risk_features
                                                        +---------------> gold.analytics_monthly
```

Dagster orchestrates assets in dependency order. After:

```bash
cd financial-data-platform
poetry run dagster dev -w pipeline/workspace.yaml
```

open the URL printed by Dagster, usually `http://127.0.0.1:3000`.

Postgres schemas `bronze`, `silver`, and `gold` are created on first Docker start by `init_schemas.sql`.

## Step 1 - Source Data

| Item | Value |
|---|---|
| File | `seed_data/generate_sqlite.py` |
| Command | `poetry run python seed_data/generate_sqlite.py` |
| Output | `seed_data/financial_data.sqlite` |

The script creates four SQLite tables that simulate Oracle query results:

| Table | Rows | Primary key |
|---|---:|---|
| `customer_accounts` | 500 | `account_id` |
| `transactions` | 5,000 | `transaction_id` |
| `risk_flags` | 300 | `flag_id` |
| `monthly_summaries` | 200 | `summary_id` |

The generator seeds Faker and Python random with `42`, but it also includes a current timestamp marker in a small number of generated rows. That means each regeneration is mostly reproducible in shape, but not byte-for-byte identical.

`OracleConnector` reads these SQLite tables through named query results in `connectors/connectors/oracle_connector.py`. In production, the connector implementation or settings would be swapped for real Oracle access while preserving the asset boundary.

## Step 2 - Runtime Configuration

| File | Role |
|---|---|
| `pipeline/pipeline/project_env.py` | Loads `financial-data-platform/.env` with `override=False` |
| `pipeline/pipeline/__init__.py` | Builds Dagster `Definitions` and resources from `os.getenv(...)` |
| `pipeline/pipeline/config/client_config.py` | Loads and validates per-client YAML configs |
| `pipeline/pipeline/config/clients/client_a.yaml` | Example multi-client config |

Runtime today:

```text
.env / shell / CI
    -> os.getenv(...)
    -> OracleResource and PostgresResource
    -> assets receive oracle: and postgres: resources
```

The YAML config layer is scaffolded and validated, but it is not yet consumed by Dagster runtime. `load_active_client_config()` can load `CLIENT_ID`, resolve `${ENV_VAR}` placeholders, and validate with Pydantic, but assets and resources currently use environment variables directly.

## Step 3 - Connectors And Resources

```text
connectors/connectors/base.py
    -> BaseConnector

connectors/connectors/oracle_connector.py
    -> OracleConnector
    -> SQLite-backed local reads through QUERY_MAP

connectors/connectors/postgres_connector.py
    -> PostgresConnector
    -> Postgres read/write helpers

pipeline/pipeline/resources/db_resources.py
    -> OracleResource and PostgresResource
    -> Dagster ConfigurableResource wrappers
```

Assets do not create engines directly. They call `oracle.get_connector()` or `postgres.get_connector()` and exchange pandas DataFrames across the connector boundary.

## Step 4 - Bronze Layer

| Item | Value |
|---|---|
| File | `pipeline/pipeline/assets/bronze.py` |
| Helpers | `pipeline/pipeline/utils/hashing.py` |
| Target | `bronze.<source_table>` |
| Write mode | Append changed rows |

Bronze assets:

- `bronze_customer_accounts`
- `bronze_transactions`
- `bronze_risk_flags`
- `bronze_monthly_summaries`

Each asset calls `materialize_bronze_table()`:

```text
oracle.read_table(table_name)
    -> returns full local SQLite result set
    -> compute _row_hash from source columns
    -> read current Bronze hashes by primary key
    -> keep rows where pk is new or hash differs
    -> deactivate previous current versions for changed keys
    -> append changed rows with metadata
```

Bronze metadata columns:

- `_row_hash`
- `_ingested_at`
- `_source_query`
- `_is_current`

Important production note: Bronze avoids appending unchanged rows, but the local implementation still reads the full SQLite result set before filtering. For a real Oracle source with millions of rows, the extraction query should be incremental using CDC, watermarks, logs, or a client-provided change table.

## Step 5 - Hashing Helpers

| Function | Purpose |
|---|---|
| `compute_row_hash(row, exclude_cols)` | Normalises row values, excludes metadata columns, JSON serialises, and returns MD5 |
| `get_existing_hashes(engine, table, pk_col)` | Reads current Bronze hashes keyed by source primary key |
| `filter_changed_rows(df, existing_hashes, pk_col)` | Keeps new or changed rows only |

The hashing strategy assumes source primary keys are stable.

## Step 6 - Silver Layer

| Item | Value |
|---|---|
| File | `pipeline/pipeline/assets/silver.py` |
| Target | `silver.<source_table>` |
| Write mode | Replace |

Silver dependencies:

```text
bronze_customer_accounts   -> silver_customer_accounts
bronze_transactions        -> silver_transactions
bronze_risk_flags          -> silver_risk_flags
bronze_monthly_summaries   -> silver_monthly_summaries
```

`process_silver_table()` does this:

```text
read current Bronze rows where _is_current = true
    -> normalise column names to snake_case
    -> sort by _ingested_at
    -> drop duplicate primary keys, keeping latest
    -> numeric nulls become 0
    -> string nulls become "unknown"
    -> date columns are parsed to dates
    -> resolved flag is coerced to bool, default false
    -> add _pipeline_version and _processed_at
    -> replace the Silver table
```

Silver represents the current clean state, not full history.

## Step 7 - Gold Layer

| Item | Value |
|---|---|
| File | `pipeline/pipeline/assets/gold.py` |
| Target schema | `gold` |
| Write mode | Replace |

Both Gold assets depend on all four Silver assets.

`gold_risk_features` writes `gold.risk_features`, a customer-level feature table with:

- `total_balance`
- `num_accounts`
- `days_since_last_transaction`
- `transaction_count_90d`
- `total_spend_90d`
- `num_risk_flags`
- `has_unresolved_flag`
- `max_flag_severity`
- `avg_monthly_debits`
- `credit_utilisation_ratio`
- `scoring_date`

`gold_analytics_monthly` writes `gold.analytics_monthly`, a monthly dashboard table with:

- `total_active_accounts`
- `new_accounts_opened`
- `total_transaction_volume`
- `avg_transaction_value`
- `top_merchant_category`
- `total_credits`
- `total_debits`
- `avg_credit_utilisation`
- `flagged_customers_count`

## Step 8 - Asset Checks

| Check | Asset | Rule |
|---|---|---|
| `risk_features_check` | `gold_risk_features` | Table is non-empty and `customer_id`, `scoring_date` are not null |
| `analytics_monthly_check` | `gold_analytics_monthly` | Table is non-empty and `month_year` matches `YYYY-MM` |

## Step 9 - Dagster Wiring

| File | Role |
|---|---|
| `pipeline/workspace.yaml` | Loads `python_module: pipeline` |
| `pipeline/dagster.yaml` | Local Dagster instance config |
| `pipeline/pipeline/__init__.py` | Defines assets, checks, jobs, schedules, sensors, resources |
| `pipeline/pipeline/assets/__init__.py` | Registers `ALL_ASSETS` and `ALL_ASSET_CHECKS` |
| `pipeline/pipeline/schedules/daily_schedule.py` | Defines `full_pipeline_job` and daily schedule |
| `pipeline/pipeline/sensors/change_sensor.py` | Defines source change sensor |

Dependency graph:

```text
bronze_customer_accounts ----\
bronze_transactions ---------+--> silver_* --> gold_risk_features
bronze_risk_flags -----------+             \-> gold_analytics_monthly
bronze_monthly_summaries ----/
```

Job and schedule:

- `full_pipeline_job` materialises all assets with `AssetSelection.all()`.
- `daily_full_pipeline_schedule` runs at `0 6 * * *` UTC and is running by default.

Sensor:

- `ChangeDetectionSensor`
- Minimum interval: 900 seconds
- Reads `SOURCE_SQLITE_PATH`
- Tracks row count plus MD5 checksum of 100 highest-primary-key rows per table
- Returns `SkipReason` when unchanged
- Returns `RunRequest` when changed

## Complete Call Chain

```text
poetry run dagster dev -w pipeline/workspace.yaml
    -> Dagster loads workspace.yaml
    -> imports pipeline.defs
    -> project_env loads .env if present
    -> Definitions initialise resources from environment variables
    -> user materialises all assets, or schedule/sensor fires
    -> Bronze reads source result sets, hashes, diffs, appends changes
    -> Silver reads current Bronze rows, cleans, replaces Silver tables
    -> Gold reads Silver tables, aggregates, replaces serving tables
    -> asset checks validate Gold outputs
```

## Common Mental Model Corrections

| Assumption | Actual behaviour |
|---|---|
| Bronze SQL is embedded in assets | Assets call `oracle.read_table(name)`; SQL lives in `QUERY_MAP` |
| `client_a.yaml` drives every run | It is validated but not wired into Dagster runtime yet |
| YAML uses `${POSTGRES_HOST}` etc. | Only passwords use `${...}` in the current example YAML |
| Silver string nulls become `"UNKNOWN"` | They become lowercase `"unknown"` |
| Local change detection is production-grade CDC | It is a lightweight local trigger plus Bronze hash diffing |
| Bronze never reads unchanged source rows | It does read the full local source result set, then filters before writing |

## Related Docs

- [README.md](README.md) - setup, design decisions, limitations, and assignment-oriented explanation
