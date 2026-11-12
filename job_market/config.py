"""Resolve runtime paths consistently across CLI, Airflow and Streamlit."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "analytics_dbt"
SOURCES = ("itviec", "topcv")


def _path(variable: str, default: str) -> Path:
    """Read a path override and resolve relative paths against the project root."""
    path = Path(os.environ.get(variable) or default).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    """Runtime paths and service settings shared by pipeline stages and the app."""

    duckdb_path: Path
    data_dir: Path
    qdrant_path: Path
    qdrant_collection: str
    flaresolverr_url: str

    @property
    def landing_dir(self) -> Path:
        """Return the directory for raw and enriched Parquet batches."""
        return self.data_dir / "landing"

    @property
    def archive_dir(self) -> Path:
        """Return the directory for batches already loaded into Bronze."""
        return self.data_dir / "archive"


def load_settings() -> Settings:
    """Load project defaults from .env without overriding exported variables."""
    load_dotenv(PROJECT_ROOT / ".env")
    return Settings(
        duckdb_path=_path("DUCKDB_PATH", "job_market.duckdb"),
        data_dir=_path("JOB_MARKET_DATA_DIR", "data"),
        qdrant_path=_path("QDRANT_PATH", "local_qdrant_db"),
        qdrant_collection=os.getenv("QDRANT_COLLECTION", "all_it_jobs_v6"),
        flaresolverr_url=os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1"),
    )
