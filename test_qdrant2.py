import os
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, Range, MatchValue, IsNullCondition, PayloadField

client = QdrantClient(path="local_qdrant_db")
count = client.count(collection_name="all_it_jobs_v6").count
print(f"Total points in Qdrant: {count}")

filter1 = Filter(
    must=[
        FieldCondition(
            key="metadata.is_active",
            match=MatchValue(value=True)
        )
    ]
)
res1 = client.count(collection_name="all_it_jobs_v6", count_filter=filter1).count
print(f"Points with metadata.is_active=True: {res1}")

filter2 = Filter(
    must=[
        FieldCondition(
            key="is_active",
            match=MatchValue(value=True)
        )
    ]
)
res2 = client.count(collection_name="all_it_jobs_v6", count_filter=filter2).count
print(f"Points with is_active=True: {res2}")

# Let's see the payload of 1 point
points = client.scroll(collection_name="all_it_jobs_v6", limit=1)
if points[0]:
    print("Sample payload:", points[0][0].payload)
