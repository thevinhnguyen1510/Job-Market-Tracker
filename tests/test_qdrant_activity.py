import unittest

from qdrant_client import QdrantClient, models

from job_market.search.activity import sync_existing_job_activity
from job_market.search.matching import CandidateProfile, search_filter


class QdrantActivityTest(unittest.TestCase):
    def test_expired_job_disappears_and_reactivation_preserves_metadata(self):
        """Verify expiration hides a point and reactivation restores it without rewriting vectors."""
        client = QdrantClient(":memory:")
        self.addCleanup(client.close)
        collection = "jobs"
        job_id = "00000000-0000-0000-0000-000000000001"
        missing_id = "00000000-0000-0000-0000-000000000002"
        client.create_collection(
            collection, vectors_config=models.VectorParams(size=2, distance=models.Distance.COSINE)
        )
        client.upsert(
            collection,
            points=[
                models.PointStruct(
                    id=job_id,
                    vector=[1.0, 0.0],
                    payload={
                        "is_active": True,  # Legacy flag at the root.
                        "page_content": "Data Engineer | Python",
                        "metadata": {
                            "is_active": True,
                            "status": "Active",
                            "job_url": "https://example.test/jobs/1",
                            "yoe": 2,
                        },
                    },
                )
            ],
        )
        active_filter = search_filter(CandidateProfile(yoe=1))

        self.assertEqual(client.count(collection, count_filter=active_filter).count, 1)

        for is_active in (False, False, True):
            with self.subTest(is_active=is_active):
                self.assertEqual(
                    sync_existing_job_activity(
                        client, collection, [job_id, missing_id], is_active=is_active
                    ),
                    1,
                )
                self.assertEqual(client.count(collection, count_filter=active_filter).count, int(is_active))
                self.assertEqual(client.count(collection).count, 1)
                point = client.retrieve(collection, ids=[job_id], with_vectors=True)[0]
                self.assertEqual(point.vector, [1.0, 0.0])
                self.assertEqual(point.payload["page_content"], "Data Engineer | Python")
                self.assertEqual(point.payload["metadata"]["job_url"], "https://example.test/jobs/1")
                self.assertEqual(point.payload["metadata"]["status"], "Active" if is_active else "Inactive")
