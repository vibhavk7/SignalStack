# Connectors

Reusable database connectivity package for the financial data platform.

The package exposes:

- `OracleConnector`: an Oracle-like read connector. In local development it is backed by SQLite so the pipeline can run without an Oracle client.
- `PostgresConnector`: a SQLAlchemy 2.0 based reader/writer for Postgres schemas and tables.
- Pydantic v2 settings models for connection configuration.

The connector APIs intentionally pass pandas `DataFrame` objects at the boundary because the pipeline layer performs pandas-based Bronze, Silver, and Gold transformations.
