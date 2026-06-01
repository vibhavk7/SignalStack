"""YAML backed per-client configuration models."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
CONFIG_DIR = Path(__file__).resolve().parent / "clients"


class OracleClientConfig(BaseModel):
    """Client Oracle connection configuration."""

    host: str
    port: int
    service_name: str
    username: str
    password: str


class PostgresClientConfig(BaseModel):
    """Client Postgres connection configuration."""

    host: str
    port: int
    database: str
    username: str
    password: str


class PipelineClientConfig(BaseModel):
    """Enabled client pipelines."""

    enabled: list[str] = Field(default_factory=list)


class ScheduleClientConfig(BaseModel):
    """Client schedule settings."""

    ingestion_cron: str


class FeatureFlagConfig(BaseModel):
    """Client-specific feature flags."""

    enable_risk_gold: bool = True
    enable_analytics_gold: bool = True


class ClientConfig(BaseModel):
    """Complete validated client configuration."""

    client_id: str
    oracle: OracleClientConfig
    postgres: PostgresClientConfig
    pipelines: PipelineClientConfig
    schedules: ScheduleClientConfig
    feature_flags: FeatureFlagConfig


def _resolve_env_refs(value: Any) -> Any:
    """Recursively replace ${VAR} references using environment variables."""

    if isinstance(value, dict):
        return {key: _resolve_env_refs(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_env_refs(item) for item in value]
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            env_name = match.group(1)
            if env_name not in os.environ:
                raise ValueError(f"Environment variable '{env_name}' is required by client config")
            return os.environ[env_name]

        return ENV_VAR_PATTERN.sub(replace, value)
    return value


def load_client_config(client_id: str, config_dir: Path = CONFIG_DIR) -> ClientConfig:
    """Load and validate a named client YAML config."""

    path = config_dir / f"{client_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Client config not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    resolved = _resolve_env_refs(raw)
    return ClientConfig.model_validate(resolved)


def load_active_client_config(config_dir: Path = CONFIG_DIR) -> ClientConfig:
    """Load the active client config from CLIENT_ID, defaulting to client_a."""

    return load_client_config(os.getenv("CLIENT_ID", "client_a"), config_dir=config_dir)
