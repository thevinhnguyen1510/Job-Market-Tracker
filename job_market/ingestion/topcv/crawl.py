"""Collect TopCV job cards with a browser, then write a raw batch."""

import logging
import random
import re
import time
from contextlib import suppress
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from job_market.config import Settings
from job_market.ingestion.landing import SEARCH_KEYWORDS, job_record, write_raw_batch

logger = logging.getLogger(__name__)
BASE_URL = "https://www.topcv.vn/"
MAX_PAGES = 10
MAX_ATTEMPTS = 10
PAGES_PER_BROWSER = 5


def parse_card(card: Tag, keyword: str) -> dict[str, str] | None:
    """Parse a job card, preferring the numeric ID in the posting URL."""
    title = card.find("h3") or card.select_one("a[class*='title']")
    if not title:
        return None
    link_tag = title if title.name == "a" else card.find("a")
    if not link_tag or not link_tag.get("href"):
        return None
    link = urljoin(BASE_URL, link_tag["href"]).split("?", 1)[0]
    external_id = re.search(r"/(\d+)\.html", link)
    company = card.select_one("a[class*='company']")
    location = card.select_one("label[class*='address']")
    salary = card.select_one("label[class*='salary']")
    return job_record(
        source="TopCV",
        keyword=keyword,
        job_id=external_id.group(1) if external_id else link,
        job_url=link,
        job_title=title.get_text(strip=True),
        company_name=company.get_text(strip=True) if company else "Unknown company",
        location=location.get_text(strip=True) if location else "Unknown location",
        salary_raw=salary.get_text(strip=True) if salary else "Thỏa thuận",
        tech_stack="To be extracted by AI",
    )


def fetch_page(driver, keyword: str, page: int) -> BeautifulSoup:
    """Fetch and parse a listing page, retrying blocked or failed loads."""
    url = f"{BASE_URL}tim-viec-lam-{keyword}?page={page}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            driver.uc_open_with_reconnect(url, reconnect_time=6)
            time.sleep(random.uniform(4.5, 6.5))
            driver.set_window_size(1920, 1080)
            time.sleep(1)
            driver.uc_gui_click_captcha()
            time.sleep(2)
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            if "Access denied" not in html and "Cloudflare" not in (soup.title.text if soup.title else ""):
                return soup
            reason, delay = "blocked", 15
        except Exception as exc:
            reason, delay = type(exc).__name__, 10
        logger.warning("keyword=%s page=%d attempt=%d fetch failed: %s", keyword, page, attempt, reason)
        if attempt < MAX_ATTEMPTS:
            time.sleep(delay)
    raise RuntimeError(f"TopCV listing unavailable: keyword={keyword} page={page}")


def run(settings: Settings) -> int:
    """Collect and deduplicate TopCV jobs before writing a raw batch."""
    from sbvirtualdisplay import Display
    from seleniumbase import Driver

    display = Display(visible=0, size=(1920, 1080))
    display.start()
    driver = None
    jobs = {}
    pages_in_session = 0
    failures = 0
    try:
        for keyword in SEARCH_KEYWORDS:
            for page in range(1, MAX_PAGES + 1):
                if driver is None or pages_in_session >= PAGES_PER_BROWSER:
                    if driver is not None:
                        with suppress(Exception):
                            driver.quit()
                        time.sleep(random.uniform(8.0, 12.0))
                    # GUI interactions need a headed browser inside Xvfb.
                    driver = Driver(uc=True, headless=False, no_sandbox=True, browser="chrome")
                    pages_in_session = 0
                try:
                    soup = fetch_page(driver, keyword, page)
                except RuntimeError as exc:
                    failures += 1
                    logger.warning("%s", exc)
                    break
                pages_in_session += 1
                before = len(jobs)
                for card in soup.select("div.job-item-search-result"):
                    if (job := parse_card(card, keyword)) is not None:
                        jobs.setdefault(job["job_id"], job)
                added = len(jobs) - before
                logger.info("keyword=%s page=%d new_jobs=%d", keyword, page, added)
                if not added:
                    break
                time.sleep(random.uniform(5.5, 9.5))
    finally:
        if driver is not None:
            with suppress(Exception):
                driver.quit()
        display.stop()

    if not jobs:
        logger.warning("No TopCV jobs collected; failed_keywords=%d", failures)
        return int(failures > 0)
    path = write_raw_batch(settings, "topcv", list(jobs.values()))
    logger.info("jobs=%d failed_keywords=%d raw_batch=%s", len(jobs), failures, path)
    return 0
