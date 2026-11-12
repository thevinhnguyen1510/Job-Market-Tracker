"""Inspect runtime storage without writing warehouse data."""

import duckdb
from qdrant_client import QdrantClient, models

from job_market.config import Settings
from job_market.search.activity import ACTIVE_FIELD


def run(settings: Settings, target: str) -> int:
    """Print DuckDB table summaries or Qdrant point counts for diagnosis."""
    if target == "warehouse":
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
            print(f"Warehouse: {settings.duckdb_path}")
            print(conn.execute("SHOW TABLES").df().to_string(index=False))
            if conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'silver_jobs'"
            ).fetchone()[0]:
                print(conn.execute("""
                    SELECT source, status, COUNT(*) AS jobs
                    FROM silver_jobs GROUP BY source, status ORDER BY source, status
                """).df().to_string(index=False))
        return 0

    if not settings.qdrant_path.is_dir():
        raise FileNotFoundError(f"Qdrant data directory not found: {settings.qdrant_path}")
    client = QdrantClient(path=str(settings.qdrant_path))
    try:
        collection = settings.qdrant_collection
        if not client.collection_exists(collection):
            raise RuntimeError(f"Qdrant collection not found: {collection}")
        active = models.Filter(must=[
            models.FieldCondition(key=ACTIVE_FIELD, match=models.MatchValue(value=True))
        ])
        print(f"Collection: {collection} ({settings.qdrant_path})")
        print(f"Points: {client.count(collection).count}; active: {client.count(collection, count_filter=active).count}")
    finally:
        client.close()
    return 0
