"""Verify older listings and flag confirmed expirations in Bronze."""

import logging
import random
import time
from collections import defaultdict

import duckdb
import requests
from curl_cffi import requests as browser_requests

from job_market.config import SOURCES, Settings
from job_market.ingestion.itviec.enrich import parse_description as parse_itviec
from job_market.ingestion.landing import CONNECTION_ERROR, EXPIRED
from job_market.ingestion.topcv.enrich import parse_description as parse_topcv

logger = logging.getLogger(__name__)
MIN_DAYS_OPEN = 7
UPDATE_BATCH_SIZE = 500


def fetch_listing(url: str, source: str, flaresolverr_url: str) -> tuple[int, str]:
    """Return a posting's HTTP status and HTML, trying FlareSolverr first for TopCV."""
    if source == "topcv":
        try:
            response = requests.post(
                flaresolverr_url,
                json={"cmd": "request.get", "url": url, "maxTimeout": 30000},
                timeout=35,
            )
            response.raise_for_status()
            result = response.json()
            solution = result.get("solution", {})
            if result.get("status") == "ok" and solution.get("response"):
                return solution.get("status", 200), solution["response"]
        except requests.RequestException:
            logger.debug("FlareSolverr unavailable; trying direct fetch")
    response = browser_requests.get(url, impersonate="chrome110", timeout=15)
    return response.status_code, response.text


def is_expired(url: str, source: str, flaresolverr_url: str) -> bool:
    """Check for expiration signals and raise on an unusable response."""
    status, html = fetch_listing(url, source, flaresolverr_url)
    if status in (404, 410):
        return True
    if status != 200:
        raise RuntimeError(f"Unexpected HTTP status: {status}")
    if source == "topcv":
        description = parse_topcv(html)
        if description == CONNECTION_ERROR:
            raise RuntimeError("TopCV returned a blocked page")
        return description == EXPIRED or "tin tuyển dụng đã hết hạn" in html.lower()
    return parse_itviec(html) == EXPIRED or "việc làm này đã hết hạn" in html.lower()


def run(settings: Settings) -> int:
    """Verify older active listings and flag confirmed expirations in Bronze."""
    with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
        jobs = conn.execute("""
            SELECT job_id, job_url, lower(source)
            FROM silver_jobs
            WHERE status = 'Active' AND days_open >= ?
            ORDER BY days_open DESC, source, job_id
        """, [MIN_DAYS_OPEN]).fetchall()

    expired = defaultdict(list)
    failures = 0
    for job_id, url, source in jobs:
        if source not in SOURCES:
            logger.warning("job_id=%s unsupported source=%s", job_id, source)
            failures += 1
            continue
        try:
            if is_expired(url, source, settings.flaresolverr_url):
                expired[source].append(job_id)
        except (RuntimeError, browser_requests.RequestsError) as exc:
            failures += 1
            logger.warning("source=%s job_id=%s verification failed: %s", source, job_id, type(exc).__name__)
        time.sleep(random.uniform(0.6, 1.2))

    total_expired = sum(map(len, expired.values()))
    if total_expired:
        # Write only confirmed expirations, after all network requests are done.
        with duckdb.connect(str(settings.duckdb_path)) as conn:
            for source, ids in expired.items():
                for offset in range(0, len(ids), UPDATE_BATCH_SIZE):
                    batch = ids[offset:offset + UPDATE_BATCH_SIZE]
                    conn.execute(
                        f"UPDATE raw_{source}_jobs SET job_description = ? WHERE job_id IN (SELECT unnest(?))",
                        [EXPIRED, batch],
                    )
        logger.info("Bronze updated; rebuild and test dbt models before syncing search")
    logger.info("checked=%d expired=%d errors=%d", len(jobs), total_expired, failures)
    return int(bool(jobs) and failures == len(jobs))
