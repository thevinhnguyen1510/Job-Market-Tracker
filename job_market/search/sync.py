"""Publish job summaries and activity from Silver to Qdrant."""

import hashlib
import logging
import uuid

import duckdb
from langchain_core.documents import Document

from job_market.config import Settings
from job_market.search.activity import sync_existing_job_activity
from job_market.search.index import connect, ensure_collection, hybrid_store

logger = logging.getLogger(__name__)


def point_id(job_id: str) -> str:
    """Derive the same Qdrant point ID on every sync for a given job ID."""
    return str(uuid.UUID(hashlib.md5(str(job_id).encode()).hexdigest()))


def job_document(row) -> Document:
    """Build the searchable summary and filter metadata from a Silver job row."""
    content = (
        f"Source: {row['source']} | Title: {row['job_title']} | "
        f"Tech: {row['ai_core_tech_stack']} | Level: {row['job_level']} | "
        f"Exp: {row['min_years_of_experience']} years | English: {row['english_requirement']}"
    )
    return Document(
        page_content=content,
        metadata={
            "job_id": row["job_id"],
            "job_title": row["job_title"],
            "job_url": row["job_url"],
            "company_name": str(row["company_name"]) if row.get("company_name") else "Tech Company",
            "ai_job_role": row["ai_job_role"],
            "ai_core_tech_stack": row["ai_core_tech_stack"],
            "job_level": row["job_level"],
            "english_requirement": row["english_requirement"],
            "salary_raw": str(row["salary_raw"]) if row.get("salary_raw") else "Negotiable",
            "yoe": row["min_years_of_experience"],
            "source": row["source"],
            "status": row["status"],
            "is_active": True,
            "first_seen_at": str(row["first_seen_at"]) if row.get("first_seen_at") is not None else "",
            "days_open": int(row["days_open"]) if row.get("days_open") is not None else 0,
        },
    )


def run(settings: Settings) -> int:
    """Sync activity and publish either all active jobs or today's extractions."""
    collection = settings.qdrant_collection
    client = connect(settings)
    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
            full_load = ensure_collection(client, collection)
            inactive_ids = [
                point_id(job_id)
                for (job_id,) in conn.execute("""
                    SELECT job_id FROM silver_all_jobs WHERE status IS DISTINCT FROM 'Active'
                """).fetchall()
            ]
            inactive_count = sync_existing_job_activity(client, collection, inactive_ids, is_active=False)

            query = "SELECT * FROM silver_all_jobs WHERE status = 'Active'"
            # This calendar-day filter is not a durable checkpoint.
            if not full_load:
                query += " AND DATE(processed_at) = CURRENT_DATE"
            jobs = conn.execute(query).df()
            active_ids = [
                point_id(job_id)
                for (job_id,) in conn.execute("""
                    SELECT job_id FROM silver_all_jobs WHERE status = 'Active'
                """).fetchall()
            ]

        # Release the DuckDB file before calling the embedding API.
        if not jobs.empty:
            documents = [job_document(row) for _, row in jobs.iterrows()]
            hybrid_store(client, collection).add_documents(
                documents=documents, ids=[point_id(job_id) for job_id in jobs["job_id"]]
            )
        active_count = sync_existing_job_activity(client, collection, active_ids, is_active=True)
        logger.info(
            "collection=%s full_load=%s upserted=%d inactive=%d active=%d",
            collection, full_load, len(jobs), inactive_count, active_count,
        )
    finally:
        client.close()
    return 0
