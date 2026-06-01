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

## Prerequisites

- **Python** 3.11 or 3.12
- **[Poetry](https://python-poetry.org/docs/#installation)** for dependency management
- **Docker** (for local Postgres)

## Where To Run Commands

This folder (`financial-data-platform/`) is the **working directory** for every setup command below.

| Path | Purpose |
|------|---------|
| `financial-data-platform/` | Poetry, Docker, `.env`, tests, and Dagster (`poetry run …`) |
| `financial-data-platform/pipeline/` | Dagster code and `workspace.yaml` — do **not** `cd` here to run Poetry or `dagster dev` |
| `financial-data-platform/connectors/` | Connector library (installed via root `poetry install`) |

From a fresh clone of the `SignalStack` repo:

```bash
cd financial-data-platform   # or: cd ~/SignalStack/financial-data-platform
```

If your shell prompt already shows `financial-data-platform`, you are in the right place — skip the `cd`.

Relative paths such as `seed_data/financial_data.sqlite` and `pipeline/workspace.yaml` are resolved from this directory.

## Local Setup

1. Install dependencies:

   ```bash
   poetry install
   ```

2. Configure local credentials (choose one):

   **Option A — `.env` file (recommended for local dev)**

   ```bash
   cp .env.example .env
   ```

   Variables are loaded automatically from `.env` in this directory when you run tests or Dagster. Shell exports and CI secrets still override `.env` values if both are set.

   **Option B — shell exports**

   ```bash
   export POSTGRES_PASSWORD=postgres
   export POSTGRES_USERNAME=postgres
   export POSTGRES_DATABASE=financial_platform
   export CLIENT_A_ORACLE_PASSWORD=local-dev-password
   export SOURCE_SQLITE_PATH=seed_data/financial_data.sqlite
   ```

3. Generate the SQLite source database:

   ```bash
   poetry run python seed_data/generate_sqlite.py
   ```

   Creates `seed_data/financial_data.sqlite` (gitignored; regenerate after clone).

4. Start Postgres:

   ```bash
   docker compose up -d postgres
   ```

   Optional: `docker compose ps` or `docker compose logs -f postgres`

5. Run the tests:

   ```bash
   poetry run pytest
   ```

6. Launch Dagster:

   ```bash
   poetry run dagster dev -w pipeline/workspace.yaml
   ```

   Run from `financial-data-platform/` so Poetry uses the root virtualenv (which includes `dagster` and `dagster-webserver`). Open the URL printed in the terminal (usually `http://127.0.0.1:3000`).

7. Materialize all assets in the Dagster UI, or run the daily schedule. The schedule is configured for `06:00 UTC`.

### Quick start (copy-paste)

```bash
cd ~/SignalStack/financial-data-platform
poetry install
cp .env.example .env
poetry run python seed_data/generate_sqlite.py
docker compose up -d postgres
poetry run pytest
poetry run dagster dev -w pipeline/workspace.yaml
```

### Common mistakes

| Mistake | Fix |
|---------|-----|
| `cd financial-data-platform/pipeline` then `poetry run dagster …` | Stay in `financial-data-platform/`; use `-w pipeline/workspace.yaml` |
| `cd pipeline` and `poetry run dagster` | Poetry may use the `pipeline/` subproject and report `Command not found: dagster` |
| `cd financial-data-platform` when already inside it | Skip the extra `cd`; run commands in the current directory |
| Committing `.env`, `*.sqlite`, or `__pycache__/` | These are gitignored; use `.env.example` as the template |

## Adding A New Client

1. Create `pipeline/pipeline/config/clients/<client_id>.yaml`.
2. Match the structure in `client_a.yaml`, including Oracle, Postgres, enabled pipeline names, schedule, and feature flags.
3. Use `${ENV_VAR}` references for secrets rather than committing secret values.
4. Set `CLIENT_ID=<client_id>` in `.env` (or `export CLIENT_ID=<client_id>`).
5. Add any new secret variables referenced in the YAML to `.env` or your shell environment before running Dagster.

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
