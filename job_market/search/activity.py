from collections.abc import Sequence

from qdrant_client import QdrantClient

METADATA_KEY = "metadata"
ACTIVE_FIELD = f"{METADATA_KEY}.is_active"


def sync_existing_job_activity(
    client: QdrantClient,
    collection_name: str,
    point_ids: Sequence[str],
    *,
    is_active: bool,
) -> int:
    """Update the metadata used by search without changing vectors or other fields."""
    batch_size = 500
    updated_count = 0
    for offset in range(0, len(point_ids), batch_size):
        existing = client.retrieve(
            collection_name=collection_name,
            ids=point_ids[offset:offset + batch_size],
            with_payload=False,
        )
        if not existing:
            continue
        client.set_payload(
            collection_name=collection_name,
            key=METADATA_KEY,
            payload={
                "is_active": is_active,
                "status": "Active" if is_active else "Inactive",
            },
            points=[point.id for point in existing],
            wait=True,
        )
        updated_count += len(existing)
    return updated_count
