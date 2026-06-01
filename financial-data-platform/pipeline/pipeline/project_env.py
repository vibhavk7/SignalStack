"""Load local environment variables from the monorepo root `.env` file."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env"
_loaded = False


def load_project_env() -> Path | None:
    """Load `.env` from `financial-data-platform/` if present.

    Existing environment variables are not overwritten so shell exports and
    CI secrets still take precedence.
    """

    global _loaded
    if _loaded:
        return _ENV_FILE if _ENV_FILE.exists() else None
    _loaded = True
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)
        return _ENV_FILE
    return None


load_project_env()
