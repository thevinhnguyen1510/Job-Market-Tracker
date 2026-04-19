from qdrant_client import QdrantClient

# Trỏ vào thư mục DB của bạn
client = QdrantClient(path="local_qdrant_db")
COLLECTION_NAME = "all_it_jobs_v6"

print("🔍 ĐANG KIỂM TRA QUẢN TRỊ QDRANT...\n")

if client.collection_exists(COLLECTION_NAME):
    # 1. Đếm tổng số Jobs đang có trong DB
    collection_info = client.get_collection(COLLECTION_NAME)
    total_jobs = collection_info.points_count
    print(f"✅ TỔNG SỐ JOBS TRONG DB: {total_jobs}")
    
    if total_jobs > 0:
        # 2. Lấy thử 1 Job ra xem Metadata nó lưu cái gì
        print("\n🔎 XEM THỬ METADATA CỦA 1 JOB BẤT KỲ:")
        sample_job, _ = client.scroll(
            collection_name=COLLECTION_NAME,
            limit=1
        )
        for point in sample_job:
            print(f"- ID: {point.id}")
            print(f"- Payload (Metadata): {point.payload}")
else:
    print("❌ BÁO ĐỘNG: Collection không tồn tại! Tức là file sync_qdrant.py chưa lưu được gì cả.")