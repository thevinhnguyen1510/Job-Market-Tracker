"""Fetch ITviec descriptions and classify expired postings."""

from bs4 import BeautifulSoup
from curl_cffi import requests

from job_market.config import Settings
from job_market.ingestion.landing import CONNECTION_ERROR, EXPIRED, MISSING_DESCRIPTION, enrich_latest_batch


def parse_description(html: str) -> str:
    """Extract the description or return an expired or missing-content marker."""
    soup = BeautifulSoup(html, "html.parser")
    actions = soup.select_one(".job-actions")
    if actions and (actions.select_one(".bg-light-warning-color") or "expired" in actions.get_text().lower()):
        return EXPIRED
    content = soup.select_one("section[class*='job-content']")
    return content.get_text(separator="\n", strip=True) if content else MISSING_DESCRIPTION


def fetch_description(url: str) -> str:
    """Fetch an ITviec posting, treating HTTP 404 and 410 as expired."""
    try:
        response = requests.get(url, impersonate="chrome110", timeout=30)
    except requests.RequestsError:
        return CONNECTION_ERROR
    if response.status_code in (404, 410):
        return EXPIRED
    if response.status_code != 200:
        return f"Error {response.status_code}"
    return parse_description(response.text)


def run(settings: Settings) -> int:
    """Enrich the newest ITviec batch with job descriptions."""
    return enrich_latest_batch(settings, "itviec", fetch_description, delay=(1.5, 3.0))
