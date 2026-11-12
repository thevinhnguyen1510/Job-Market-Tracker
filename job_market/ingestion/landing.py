"""Shared raw-batch schema and Parquet output."""

import logging
import random
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pandas as pd

from job_market.config import Settings

logger = logging.getLogger(__name__)
EXPIRED = "EXPIRED"
CONNECTION_ERROR = "Connection error"
MISSING_DESCRIPTION = "JD content not found"
SEARCH_KEYWORDS = (
    "data-engineer", "data-analyst", "data-scientist", "analytics-engineer",
    "machine-learning", "ai-engineer", "business-intelligence",
)


def job_record(*, keyword: str, source: str, **fields: str) -> dict[str, str]:
    """Add shared crawl metadata to a source-specific job record."""
    return {
        **fields,
        "source": source,
        "crawl_timestamp": datetime.now().isoformat(sep=" ", timespec="seconds"),
        "experience_level": "",
        "job_category": keyword,
        "job_description": "",
    }


def write_raw_batch(settings: Settings, source: str, jobs: list[dict[str, str]]) -> Path:
    """Write the source's raw listings to Parquet and return the file path."""
    directory = settings.landing_dir / source
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = directory / f"{source}_raw_{stamp}.parquet"
    pd.DataFrame(jobs).to_parquet(path, index=False, compression="snappy")
    return path


def enrich_latest_batch(
    settings: Settings, source: str, fetch: Callable[[str], str], *, delay: tuple[float, float]
) -> int:
    """Enrich the newest raw batch and remove it only after writing the output."""
    directory = settings.landing_dir / source
    raw_files = sorted(directory.glob(f"{source}_raw_*.parquet"))
    if not raw_files:
        logger.info("source=%s no raw batches to enrich", source)
        return 0
    path = raw_files[-1]
    jobs = pd.read_parquet(path)
    if jobs.empty:
        logger.warning("source=%s empty batch=%s", source, path)
        return 0
    logger.info("source=%s raw_batch=%s jobs=%d", source, path, len(jobs))
    descriptions = []
    for job_id, url in zip(jobs["job_id"], jobs["job_url"]):
        description = fetch(url)
        descriptions.append(description)
        if description in {CONNECTION_ERROR, MISSING_DESCRIPTION} or description.startswith("Error "):
            logger.warning("source=%s job_id=%s fetch_result=%s", source, job_id, description)
        time.sleep(random.uniform(*delay))

    jobs["job_description"] = descriptions
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    enriched = directory / f"{source}_enriched_{stamp}.parquet"
    jobs.to_parquet(enriched, index=False, compression="snappy")
    path.unlink()  # Keep the raw input until the enriched batch is written.
    outcomes = Counter(descriptions)
    logger.info(
        "source=%s enriched_batch=%s jobs=%d expired=%d connection_errors=%d missing_descriptions=%d",
        source, enriched, len(jobs), outcomes[EXPIRED], outcomes[CONNECTION_ERROR], outcomes[MISSING_DESCRIPTION],
    )
    return 0
