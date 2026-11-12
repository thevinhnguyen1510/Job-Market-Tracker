"""Fetch TopCV descriptions through FlareSolverr."""

from functools import partial

import requests
from bs4 import BeautifulSoup

from job_market.config import Settings
from job_market.ingestion.landing import CONNECTION_ERROR, EXPIRED, MISSING_DESCRIPTION, enrich_latest_batch


def parse_description(html: str) -> str:
    """Extract a TopCV description or classify a blocked, expired, or empty page."""
    soup = BeautifulSoup(html, "html.parser")
    if "Access denied" in html or "Cloudflare" in (soup.title.text if soup.title else ""):
        return CONNECTION_ERROR
    apply_group = soup.select_one(".box-group-button-apply")
    if soup.select_one(".box-apply-expired, .apply-expired") or (
        apply_group and "hết hạn ứng tuyển" in apply_group.get_text().lower()
    ):
        return EXPIRED
    content = (
        soup.select_one("#box-job-information-detail")
        or soup.select_one(".job-description__item")
        or soup.select_one(".box-info-job")
    )
    return content.get_text(separator="\n", strip=True)[:3500] if content else MISSING_DESCRIPTION


def fetch_description(url: str, *, flaresolverr_url: str) -> str:
    """Fetch a TopCV posting through FlareSolverr and classify its response."""
    try:
        response = requests.post(
            flaresolverr_url,
            json={"cmd": "request.get", "url": url, "maxTimeout": 60000},
            timeout=70,
        )
        response.raise_for_status()
        result = response.json()
    except requests.RequestException:
        return CONNECTION_ERROR
    if result.get("status") != "ok":
        return CONNECTION_ERROR
    return parse_description(result.get("solution", {}).get("response", ""))


def run(settings: Settings) -> int:
    """Enrich the newest TopCV batch with job descriptions."""
    fetch = partial(fetch_description, flaresolverr_url=settings.flaresolverr_url)
    return enrich_latest_batch(settings, "topcv", fetch, delay=(2.0, 4.0))
