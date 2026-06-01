# Financial Data Platform

A local data platform for a fictional financial services client. The project is a small monorepo with a reusable connectors package, a Dagster pipeline package, Docker Postgres, and a SQLite source that simulates four Oracle query result sets.

For the detailed implementation walkthrough, see [CODE_FLOW.md](CODE_FLOW.md).

## Architecture

```text
SQLite source                  Postgres target
fake Oracle results            bronze / silver / gold schemas

customer_accounts  ----\
transactions       -----+--> Dagster Bronze assets --> Silver assets --> Gold risk_features
risk_flags         -----+                                           \-> Gold analytics_monthly
monthly_summaries ----/
```

## Repository Layout

```text
SignalStack/
|-- README.md
|-- CODE_FLOW.md
`-- financial-data-platform/
    |-- pyproject.toml
    |-- docker-compose.yml
    |-- init_schemas.sql
    |-- seed_data/
    |   `-- generate_sqlite.py
    |-- connectors/
    |   `-- connectors/
    |       |-- base.py
    |       |-- config.py
    |       |-- oracle_connector.py
    |       `-- postgres_connector.py
    `-- pipeline/
        |-- workspace.yaml
        |-- dagster.yaml
        |-- tests/
        `-- pipeline/
            |-- __init__.py
            |-- project_env.py
            |-- assets/
            |-- config/
            |-- resources/
            |-- schedules/
            |-- sensors/
            `-- utils/
```

`financial-data-platform/connectors` is the reusable database connectivity library. `financial-data-platform/pipeline` contains Dagster assets, resources, schedules, sensors, config models, and tests.

## Prerequisites

- Python 3.11 or 3.12
- Poetry
- Docker

## Local Setup

Run all platform commands from `financial-data-platform/`.

```bash
cd financial-data-platform
poetry install
cp .env.example .env
poetry run python seed_data/generate_sqlite.py
docker compose up -d postgres
poetry run pytest
poetry run dagster dev -w pipeline/workspace.yaml
```

Open the Dagster URL printed by the terminal, usually `http://127.0.0.1:3000`, then materialize all assets.

Relative paths such as `seed_data/financial_data.sqlite` and `pipeline/workspace.yaml` are resolved from `financial-data-platform/`.

## Configuration

Runtime connections are currently driven by environment variables loaded from `financial-data-platform/.env`, shell exports, or CI secrets.

Important variables:

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`
- `CLIENT_A_ORACLE_PASSWORD`, `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE_NAME`, `ORACLE_USERNAME`
- `SOURCE_SQLITE_PATH`, defaulting to `seed_data/financial_data.sqlite`

There is also a per-client YAML layer in `financial-data-platform/pipeline/pipeline/config/clients/client_a.yaml`. It validates connection fields, enabled pipelines, feature flags, and schedule cadence through Pydantic. That layer is scaffolded for multi-client deployment, but Dagster runtime does not consume it yet; `pipeline/pipeline/__init__.py` currently wires resources directly from `os.getenv(...)`.

## Data Layers

Bronze stores the four source result sets with source columns preserved plus ingestion metadata: `_ingested_at`, `_row_hash`, `_is_current`, and `_source_query`. It appends only new or changed rows and marks previous versions non-current.

Silver reads current Bronze rows, normalises column names, handles nulls, deduplicates by primary key, and stamps `_pipeline_version` plus `_processed_at`.

Gold serves two outputs:

- `gold.risk_features`: customer-level recency, frequency, monetary, flag, balance, and utilisation features for risk scoring.
- `gold.analytics_monthly`: monthly account, transaction, credit/debit, utilisation, and flag summaries for dashboard consumption.

## Orchestration

Dagster uses assets as the primary abstraction. The pipeline defines:

- Four Bronze assets, one per source result set.
- Four Silver assets, each dependent on its Bronze source.
- Two Gold assets, each dependent on all Silver tables.
- `full_pipeline_job` selecting all assets.
- `daily_full_pipeline_schedule`, running at `0 6 * * *` UTC.
- `ChangeDetectionSensor`, checking the SQLite source every 15 minutes.
- Asset checks for both Gold outputs.

## Change Detection

Bronze row-level change detection hashes the source columns for each row and compares them with the current Bronze hash for the same stable primary key. If a row is unchanged, it is not appended again. If it is new or changed, the old current version is deactivated and the new version is appended.

The local sensor is a lightweight trigger gate: it compares row counts and an MD5 checksum of the 100 highest-primary-key rows in each SQLite source table. This is enough to avoid many no-op local runs, but it can miss edits to older rows.

For a production Oracle source with millions of historical rows and only about 50 daily changes, I would prefer one of these strategies:

- Client-provided watermarks such as `updated_at` or extract batch IDs in the four result sets.
- Oracle CDC, materialized view logs, or redo-log based extraction.
- A source-side change table keyed by the query result primary keys.
- Parameterized extraction queries that only return rows changed since the last successful cursor.

The current implementation avoids rewriting unchanged rows in Postgres, but the local Bronze implementation still reads each full SQLite result set before filtering changed rows. That is a pragmatic local simulation, not the ideal production extraction pattern.

## Design Decisions

- A monorepo keeps the take-home easy to clone and run, while still separating reusable connectivity from pipeline logic.
- Connectors own database access; assets work with pandas DataFrames and do not open database engines directly.
- SQLite stands in for Oracle locally, while `OracleConnector` exposes named query-result reads so the pipeline boundary remains swappable.
- Bronze is append-only because historical versions are useful for audit and debugging.
- Silver and Gold use full replace because they are current-state and serving tables derived from Bronze history.
- Postgres schemas map naturally to Bronze, Silver, and Gold boundaries.
- Tests focus on the most critical behavior: connectors, hashing, Bronze change filtering, and Silver cleaning.

## Tests

```bash
cd financial-data-platform
poetry run pytest
```

Current local result in WSL: `6 passed`.

## Known Limitations

- The local Oracle connector does not exercise Oracle client libraries, network authentication, or Oracle-specific SQL behavior.
- Bronze assumes stable primary keys in the client-provided query results.
- The local sensor checks only row counts and recent-row checksums, so it is not a complete CDC mechanism.
- Per-client YAML config is validated but not yet wired into Dagster `Definitions`.
- Gold transformations are transparent pandas code; for much larger volumes, these would likely move closer to Postgres SQL or another scalable execution layer.
- Secrets are environment-variable based for local simplicity; production should use a secret manager.

## What I Would Do With More Time

- Wire `load_active_client_config()` into Dagster resources, schedules, and feature flags.
- Replace local full-result Bronze extraction with cursor-based or CDC-style incremental extraction.
- Add integration tests against Docker Postgres.
- Add stronger Gold data quality checks, including uniqueness and accepted ranges.
- Add typed table contracts or schema migrations instead of relying on pandas `to_sql` inference.
