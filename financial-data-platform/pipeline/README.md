# Financial Pipeline

Dagster package for the financial data platform.

## Assets

- `bronze_*`: ingest current SQLite source result sets into the Postgres `bronze` schema with hash-based change detection.
- `silver_*`: clean current Bronze rows, normalise column names, cast date/numeric columns, and replace the `silver` tables.
- `gold_risk_features`: customer-level risk scoring dataset.
- `gold_analytics_monthly`: monthly analytics dataset across the four source domains.

## Orchestration

- `full_pipeline_job`: materializes all assets.
- `daily_full_pipeline_schedule`: runs daily at `06:00 UTC`.
- `ChangeDetectionSensor`: checks SQLite every 15 minutes using row counts and recent-row checksums, then triggers the full pipeline when state changes.

## Running

```bash
poetry install
export POSTGRES_PASSWORD=postgres
export CLIENT_A_ORACLE_PASSWORD=local-dev-password
export SOURCE_SQLITE_PATH=../seed_data/financial_data.sqlite
dagster dev -w workspace.yaml
```

The package is loaded from `workspace.yaml` via `python_module: pipeline`.
