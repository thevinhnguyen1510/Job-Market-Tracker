import duckdb

print("🧹 KHỞI ĐỘNG CHIẾN DỊCH TỔNG VỆ SINH DATABASE...")

db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

try:
    # 1. XÓA CÁC BẢNG SILVER CŨ (Rác)
    conn.execute("DROP TABLE IF EXISTS silver_itviec_jobs;")
    print(" ✅ Đã xóa bảng cũ: silver_itviec_jobs")
    conn.execute("VACUUM;")
except Exception as e:
    print(f"❌ Có lỗi xảy ra: {e}")
finally:
    conn.close()