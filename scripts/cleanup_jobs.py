import os
import duckdb
from datetime import datetime

# Cấu hình ngưỡng thời gian: Nếu quá EXPIRY_DAYS ngày không được quét lại -> Hết hạn
EXPIRY_DAYS = 3

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INITIATING: CLEANUP INACTIVE JOBS...")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

if not os.path.exists(db_path):
    print(f"Error: Database file not found at {db_path}")
    exit(1)

conn = duckdb.connect(db_path)

try:
    # 1. Đếm tổng số job đang Active trước khi dọn dẹp
    active_before = conn.execute("""
        SELECT COUNT(*) FROM silver_all_jobs WHERE status = 'Active'
    """).fetchone()[0]
    print(f"-> Active jobs before scan: {active_before}")

    # 2. Đánh dấu Inactive cho các job không còn xuất hiện trong vòng EXPIRY_DAYS ngày
    # Áp dụng cho cả ITVIEC và TOPCV
    cleanup_query = f"""
        UPDATE silver_all_jobs
        SET status = 'Inactive'
        WHERE status = 'Active'
          AND last_seen_at < (CURRENT_TIMESTAMP - INTERVAL '{EXPIRY_DAYS} days')
    """
    
    conn.execute(cleanup_query)

    # 3. Thống kê chi tiết theo nguồn đã bị chuyển sang Inactive
    stats = conn.execute(f"""
        SELECT source, COUNT(*) as inactive_count
        FROM silver_all_jobs
        WHERE status = 'Inactive'
          AND last_seen_at < (CURRENT_TIMESTAMP - INTERVAL '{EXPIRY_DAYS} days')
        GROUP BY source
    """).fetchall()

    active_after = conn.execute("""
        SELECT COUNT(*) FROM silver_all_jobs WHERE status = 'Active'
    """).fetchone()[0]

    deactivated_count = active_before - active_after
    print(f"-> [OK] Successfully marked {deactivated_count} stale jobs as 'Inactive' (Threshold: > {EXPIRY_DAYS} days).")
    for source, cnt in stats:
        print(f"   - {source}: {cnt} inactive jobs")
    print(f"-> Remaining active jobs in market: {active_after}")

except Exception as e:
    print(f"[ERROR] Cleanup process failed: {e}")
    raise e
finally:
    conn.close()
    print("Cleanup task finished.")