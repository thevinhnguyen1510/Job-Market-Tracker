import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import duckdb
from bs4 import BeautifulSoup

from job_market.config import Settings
from job_market.ingestion import landing
from job_market.ingestion.itviec import crawl as itviec
from job_market.ingestion.topcv import crawl as topcv
from job_market.ingestion.topcv.enrich import parse_description
from job_market.warehouse import ingest


class IngestionTest(unittest.TestCase):
    def test_raw_batch_reaches_bronze_and_is_archived(self):
        """Verify a batch reaches Bronze and a second run does not duplicate it."""
        html = '<div class="job-card"><h3 data-url="/it-jobs/data-engineer?ref=search">Data Engineer</h3></div>'
        card = BeautifulSoup(html, "html.parser").div
        job = itviec.parse_card(card, "data-engineer")
        self.assertEqual(job["job_id"], "https://itviec.com/it-jobs/data-engineer")

        with TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings(
                duckdb_path=root / "jobs.duckdb",
                data_dir=root / "data",
                qdrant_path=root / "qdrant",
                qdrant_collection="test",
                flaresolverr_url="http://localhost:8191/v1",
            )
            raw = landing.write_raw_batch(settings, "itviec", [job])
            with patch("job_market.ingestion.landing.time.sleep"):
                landing.enrich_latest_batch(settings, "itviec", lambda _: "Build data pipelines", delay=(0, 0))
            self.assertFalse(raw.exists())
            self.assertEqual(ingest.run(settings), 0)
            self.assertEqual(ingest.run(settings), 0)
            self.assertEqual(len(list((settings.archive_dir / "itviec").glob("*.parquet"))), 1)
            with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
                rows = conn.execute("SELECT job_id, job_description FROM raw_itviec_jobs").fetchall()
            self.assertEqual(rows, [(job["job_id"], "Build data pipelines")])

    def test_topcv_parser_keeps_external_id_and_detects_expiration(self):
        """Check that parsing preserves the job ID and distinguishes expired from blocked pages."""
        html = '<div><h3><a href="/viec-lam/data-engineer/123.html?ref=list">Data Engineer</a></h3></div>'
        card = BeautifulSoup(html, "html.parser").div
        job = topcv.parse_card(card, "data-engineer")
        self.assertEqual(job["job_id"], "123")
        self.assertEqual(job["job_url"], "https://www.topcv.vn/viec-lam/data-engineer/123.html")
        self.assertEqual(parse_description('<div class="box-apply-expired"></div>'), landing.EXPIRED)
        self.assertEqual(parse_description("<title>Cloudflare</title>"), landing.CONNECTION_ERROR)
