import duckdb

print("👻 Khởi động Ghost Task: Dọn dẹp Job hết hạn...")

# 1. Kết nối vào DB (Nhớ sửa đường dẫn nếu file .duckdb nằm ở thư mục khác)
db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

try:
    # 2. Đếm số lượng Active trước khi quét
    active_before = conn.execute("SELECT COUNT(*) FROM silver_all_jobs WHERE status = 'Active'").fetchone()[0]
    print(f"📊 Số job đang Active: {active_before}")

    # 3. Thực thi câu lệnh SQL dọn dẹp
    conn.execute("""
        UPDATE silver_all_jobs 
        SET status = 'Expired'
        WHERE status = 'Active' 
          AND last_seen_at < CURRENT_TIMESTAMP - INTERVAL '3 days';
    """)

    # 4. Kiểm tra lại kết quả
    active_after = conn.execute("SELECT COUNT(*) FROM silver_all_jobs WHERE status = 'Active'").fetchone()[0]
    expired_count = active_before - active_after
    
    print(f"✅ Đã quét xong! Có {expired_count} job cũ đã bị chuyển sang trạng thái Expired.")
    print(f"📊 Số job Active hiện tại: {active_after}")

except Exception as e:
    print(f"❌ Lỗi: {e}")
finally:
    conn.close()
    print("✅ Ghost Task đã hoàn thành!")