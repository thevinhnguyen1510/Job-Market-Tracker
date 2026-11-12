"""Load enriched Parquet into Bronze and archive successfully loaded files."""

import logging
import shutil

import duckdb

from job_market.config import SOURCES, Settings

logger = logging.getLogger(__name__)


def run(settings: Settings) -> int:
    """Upsert enriched batches into Bronze and archive files after a successful write."""
    with duckdb.connect(str(settings.duckdb_path)) as conn:
        for source in SOURCES:
            # Table names come from the internal source allowlist.
            table = f"raw_{source}_jobs"
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    job_id VARCHAR PRIMARY KEY, job_url VARCHAR, job_title VARCHAR,
                    company_name VARCHAR, location VARCHAR, salary_raw VARCHAR,
                    tech_stack VARCHAR, source VARCHAR, crawl_timestamp TIMESTAMP,
                    experience_level VARCHAR, job_category VARCHAR, job_description VARCHAR,
                    first_seen_at TIMESTAMP
                )
            """)
            files = sorted((settings.landing_dir / source).glob(f"{source}_enriched_*.parquet"))
            if not files:
                logger.info("source=%s no enriched files to ingest", source)
                continue

            conn.execute(f"""
                INSERT INTO {table} (
                    job_id, job_url, job_title, company_name, location,
                    salary_raw, tech_stack, source, crawl_timestamp,
                    experience_level, job_category, job_description, first_seen_at
                )
                SELECT
                    job_id, job_url, job_title, company_name, location,
                    salary_raw, tech_stack, source, crawl_timestamp,
                    experience_level, job_category, job_description, crawl_timestamp
                FROM read_parquet(?, union_by_name = true)
                ON CONFLICT (job_id) DO UPDATE SET
                    crawl_timestamp = EXCLUDED.crawl_timestamp,
                    job_description = CASE
                        WHEN EXCLUDED.job_description NOT IN ('', 'Connection error')
                        THEN EXCLUDED.job_description
                        ELSE {table}.job_description
                    END
            """, [[path.as_posix() for path in files]])

            # The database write must succeed before any input leaves landing.
            archive = settings.archive_dir / source
            archive.mkdir(parents=True, exist_ok=True)
            for path in files:
                shutil.move(path, archive / path.name)
            logger.info("source=%s archived_batches=%d table=%s", source, len(files), table)
    return 0
