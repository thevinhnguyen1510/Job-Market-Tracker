"""Collect ITviec job cards into the raw landing zone."""

import logging
import os
import random
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from curl_cffi import requests

from job_market.config import Settings
from job_market.ingestion.landing import SEARCH_KEYWORDS, job_record, write_raw_batch

logger = logging.getLogger(__name__)
BASE_URL = "https://itviec.com/"
MAX_PAGES = 50
MAX_ATTEMPTS = 3


def parse_card(card: Tag, keyword: str) -> dict[str, str] | None:
    """Parse a job card using its URL as the ID; skip cards without a URL."""
    title = card.find("h3")
    if not title or not title.get("data-url"):
        return None
    link = urljoin(BASE_URL, title["data-url"]).split("?", 1)[0]
    company = card.select_one("span.text-hover-underline a")
    location = card.select_one("div.text-truncate[title]")
    return job_record(
        source="ITViec",
        keyword=keyword,
        job_id=link,
        job_url=link,
        job_title=title.get_text(strip=True),
        company_name=company.get_text(strip=True) if company else "Unknown company",
        location=location["title"] if location else "Unknown location",
        tech_stack=", ".join(tag.get_text(strip=True) for tag in card.select("a.itag")),
        salary_raw="Sign in to view salary" if card.select_one("a.sign-in-view-salary") else "Updating...",
    )


def fetch_page(keyword: str, page: int, headers: dict[str, str]) -> str:
    """Fetch listing HTML and raise the request error if all attempts fail."""
    url = f"{BASE_URL}it-jobs/{keyword}?page={page}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, headers=headers, impersonate="chrome110", timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestsError:
            if attempt == MAX_ATTEMPTS:
                raise
            logger.warning("keyword=%s page=%d attempt=%d request failed; retrying", keyword, page, attempt)
            time.sleep(10)
    raise AssertionError("At least one attempt is required")


def run(settings: Settings) -> int:
    """Collect and deduplicate ITviec listings, then write a raw batch."""
    cookie = os.getenv("ITVIEC_COOKIE")
    if not cookie:
        raise RuntimeError("ITVIEC_COOKIE is required to crawl ITviec")
    headers = {
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Referer": BASE_URL,
        "Cookie": cookie,
    }
    jobs = {}
    failures = 0
    for keyword in SEARCH_KEYWORDS:
        for page in range(1, MAX_PAGES + 1):
            try:
                html = fetch_page(keyword, page, headers)
            except requests.RequestsError as exc:
                failures += 1
                logger.warning("keyword=%s page=%d crawl stopped: %s", keyword, page, type(exc).__name__)
                break
            before = len(jobs)
            for card in BeautifulSoup(html, "html.parser").select("div.job-card"):
                if (job := parse_card(card, keyword)) is not None:
                    jobs.setdefault(job["job_id"], job)
            added = len(jobs) - before
            logger.info("keyword=%s page=%d new_jobs=%d", keyword, page, added)
            if not added:
                break
            time.sleep(random.uniform(3.5, 6.2))

    if not jobs:
        logger.warning("No ITviec jobs collected; failed_keywords=%d", failures)
        return int(failures > 0)
    path = write_raw_batch(settings, "itviec", list(jobs.values()))
    logger.info("jobs=%d failed_keywords=%d raw_batch=%s", len(jobs), failures, path)
    return 0
