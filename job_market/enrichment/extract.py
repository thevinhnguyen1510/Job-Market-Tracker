"""Extract and persist structured attributes for pending job descriptions."""

import asyncio
import json
import logging
import os

import duckdb
import instructor
from openai import AsyncOpenAI

from job_market.config import SOURCES, Settings
from job_market.enrichment.schema import EnglishLevel, JobExtraction

logger = logging.getLogger(__name__)
AI_EXTRACTIONS_TABLE = "raw_ai_extractions"
CONCURRENCY_LIMIT = 6


def pending_jobs(conn, source: str) -> list[tuple]:
    """Ensure the output table exists and select jobs that have no extraction yet."""
    if source not in SOURCES:
        raise ValueError(f"Unsupported source: {source}")
    raw_table = f"raw_{source}_jobs"
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {AI_EXTRACTIONS_TABLE} (
            job_id VARCHAR PRIMARY KEY,
            min_years_of_experience INTEGER,
            ai_core_tech_stack VARCHAR,
            english_requirement VARCHAR,
            ai_job_role VARCHAR,
            job_level VARCHAR,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    return conn.execute(f"""
        SELECT DISTINCT r.job_id, r.job_title, r.job_description
        FROM {raw_table} r
        LEFT JOIN {AI_EXTRACTIONS_TABLE} a ON r.job_id = a.job_id
        WHERE a.job_id IS NULL
          AND r.job_description IS NOT NULL
          AND r.job_description != ''
          AND r.job_description != 'JD content not found'
          AND r.job_description != 'Connection error'
          AND r.job_description != 'EXPIRED'
    """).fetchall()


async def extract_job(client, semaphore: asyncio.Semaphore, job: tuple) -> tuple:
    """Extract one job under the concurrency limit; return an Error row if retries fail."""
    job_id, job_title, job_desc = job
    trimmed_desc = job_desc[:2200]

    async with semaphore:
        for attempt in range(2):
            try:
                extracted: JobExtraction = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    response_model=JobExtraction,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are an elite Data Engineer and HR Tech Analyst. Extract structured data from IT Job Descriptions. "
                                "IMPORTANT: In the tech market, 'Software Engineer' and 'Software Developer' roles focusing on "
                                "server-side, microservices, databases, or languages (Java, C#, .NET, Python, Golang, C++) must be classified as 'Backend'."
                            )
                        },
                        {
                            "role": "user",
                            "content": f"Title: {job_title}\n\n### JD ###\n{trimmed_desc}"
                        }
                    ],
                    max_retries=1
                )
                return (
                    job_id,
                    extracted.min_years_of_experience,
                    json.dumps(extracted.core_tech_stack),
                    extracted.english_requirement.value,
                    extracted.job_role,
                    extracted.job_level
                )
            except Exception as e:
                if attempt == 1:
                    logger.warning("job_id=%s extraction failed: %s", job_id, type(e).__name__)
                    return (
                        job_id, 0, "[]",
                        EnglishLevel.NONE.value, "Unknown", "Error"
                    )
                await asyncio.sleep(1.0)


async def extract_pending(jobs: list[tuple], api_key: str) -> list[tuple]:
    """Process pending jobs concurrently with one API client and a shared limit."""
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async with AsyncOpenAI(api_key=api_key) as openai_client:
        client = instructor.from_openai(openai_client)
        return await asyncio.gather(*(extract_job(client, semaphore, job) for job in jobs))


def run(settings: Settings, source: str) -> int:
    """Read pending jobs, call the API without holding DuckDB open, and save results."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for job extraction")

    with duckdb.connect(str(settings.duckdb_path)) as conn:
        jobs = pending_jobs(conn, source)
    logger.info("source=%s pending_jobs=%d", source, len(jobs))
    if not jobs:
        return 0

    # Do not keep the DuckDB file open while waiting for the API.
    results = asyncio.run(extract_pending(jobs, api_key))
    with duckdb.connect(str(settings.duckdb_path)) as conn:
        conn.executemany(f"""
            INSERT INTO {AI_EXTRACTIONS_TABLE} (
                job_id, min_years_of_experience,
                ai_core_tech_stack, english_requirement, ai_job_role,
                job_level, processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (job_id) DO UPDATE SET
                min_years_of_experience = EXCLUDED.min_years_of_experience,
                ai_core_tech_stack = EXCLUDED.ai_core_tech_stack,
                english_requirement = EXCLUDED.english_requirement,
                ai_job_role = EXCLUDED.ai_job_role,
                job_level = EXCLUDED.job_level,
                processed_at = EXCLUDED.processed_at;
        """, results)
    failed = sum(row[5] == "Error" for row in results)
    logger.info("source=%s persisted=%d failed=%d", source, len(results), failed)
    return 0
