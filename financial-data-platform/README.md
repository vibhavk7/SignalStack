# Financial Data Platform

A complete local data platform for a fictional financial services client. The repo is a small monorepo with reusable database connectors, a Dagster-orchestrated pipeline, a Postgres target, and a SQLite source that simulates four Oracle query result sets.

## Architecture

```text
                         +------------------------------+
                         | SQLite source                |
                         | Oracle-compatible result sets |
                         | - customer_accounts          |
                         | - transactions               |
                         | - risk_flags                 |
                         | - monthly_summaries          |
                         +---------------+--------------+
                                         |
                                         v
+------------------+          +-------------------------+          +----------------------+
| connectors/      |          | Dagster assets          |          | Postgres             |
| - OracleConnector+--------->| Bronze ingestion        +--------->| bronze schema        |
| - PostgresConnector         | hash + version history  |          | current + history    |
+------------------+          +------------+------------+          +----------+-----------+
                                         |                                  |
                                         v                                  v
                         +-------------------------+          +----------------------+
                         | Silver assets           +--------->| silver schema        |
                         | clean, type, dedupe     |          | current clean tables |
                         +------------+------------+          +----------+-----------+
                                      |                                  |
                                      v                                  v
                         +-------------------------+          +----------------------+
                         | Gold assets             +--------->| gold schema          |
                         | risk + analytics        |          | risk_features        |
                         | checks included         |          | analytics_monthly    |
                         +-------------------------+          +----------------------+
```

## Local Setup

1. Install dependencies:

   ```bash
   cd financial-data-platform
   poetry install
   ```

2. Generate the SQLite source database:

   ```bash
   poetry run python seed_data/generate_sqlite.py
   ```

3. Start Postgres:

   ```bash
   docker compose up -d postgres
   ```

4. Export local credentials used by the client config and Dagster resources:

   ```bash
   export POSTGRES_PASSWORD=postgres
   export POSTGRES_USERNAME=postgres
   export POSTGRES_DATABASE=financial_platform
   export CLIENT_A_ORACLE_PASSWORD=local-dev-password
   export SOURCE_SQLITE_PATH=seed_data/financial_data.sqlite
   ```

5. Run the tests:

   ```bash
   poetry run pytest
   ```

6. Launch Dagster:

   ```bash
   cd pipeline
   poetry run dagster dev -w workspace.yaml
   ```

7. Materialize all assets in the Dagster UI, or run the daily schedule. The schedule is configured for `06:00 UTC`.

## Adding A New Client

1. Create `pipeline/pipeline/config/clients/<client_id>.yaml`.
2. Match the structure in `client_a.yaml`, including Oracle, Postgres, enabled pipeline names, schedule, and feature flags.
3. Use `${ENV_VAR}` references for secrets rather than committing secret values.
4. Set `CLIENT_ID=<client_id>` before loading config-dependent tooling.
5. Set the corresponding environment variables before running Dagster.

The `ClientConfig` Pydantic model validates required fields and resolves environment variable references.

## Design Decisions

- Bronze is append-only with `_is_current` flags so changed source rows preserve history without deleting prior versions.
- Row-level MD5 hashes are computed from source columns only. Metadata columns are excluded, and the primary-key stability assumption is documented in `hashing.py`.
- SQLite is used as the local Oracle stand-in, but the connector exposes Oracle-style named result sets so the pipeline boundary stays realistic.
- The Postgres connector supports real schemas in Postgres and schema-like table prefixes in SQLite tests.
- Gold datasets are rebuilt with `if_exists="replace"` because they are derived serving tables rather than history-bearing ingestion tables.
- Dagster assets are grouped by layer, with a daily schedule, a source-change sensor, and asset checks on both Gold outputs.

## Known Limitations

- The local Oracle implementation does not exercise Oracle client libraries or production network authentication.
- Bronze hash comparison assumes stable source primary keys.
- The change sensor checks row counts and recent-row checksums; a production source would ideally expose CDC, watermarks, or database logs.
- The Gold feature logic is intentionally transparent pandas code. With larger data volumes, these transformations should move into warehouse SQL or a distributed engine.
- Secrets are environment-variable based for local simplicity. A production deployment should use a secret manager.
