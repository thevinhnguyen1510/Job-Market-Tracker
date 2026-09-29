from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, Range, MatchValue

client = QdrantClient(path="local_qdrant_db")

count_all = client.count(collection_name="all_it_jobs_v6")
print("Total vectors:", count_all.count)

res = client.scroll(
    collection_name="all_it_jobs_v6",
    scroll_filter=Filter(
        must=[
            FieldCondition(
                key="is_active",
                match=MatchValue(value=True)
            )
        ]
    ),
    limit=5
)
print("With is_active filter (True):", len(res[0]))

res2 = client.scroll(
    collection_name="all_it_jobs_v6",
    scroll_filter=Filter(
        must=[
            FieldCondition(
                key="yoe",
                range=Range(lte=5)
            )
        ]
    ),
    limit=5
)
print("With yoe filter (<=5):", len(res2[0]))

res3 = client.scroll(
    collection_name="all_it_jobs_v6",
    limit=1
)
if res3[0]:
    print("Sample payload:", res3[0][0].payload)
