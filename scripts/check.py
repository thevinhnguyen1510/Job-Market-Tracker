import duckdb
import os

print("🔥 BẮT ĐẦU CHIẾN DỊCH DỌN DẸP TOÀN BỘ DATABASE...")

# Kết nối vào file DuckDB (Đảm bảo đường dẫn đúng với thư mục của bạn)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

# Nếu bạn chạy file này ở ngoài thư mục gốc, có thể dùng đường dẫn trực tiếp:
# db_path = '../job_market.duckdb' 

try:
    conn = duckdb.connect(db_path)
    
    # Danh sách toàn bộ các bảng từ mọi tầng dữ liệu
    tables_to_drop = [
        # Tầng Bronze (Raw)
        "raw_topcv_jobs",
        "raw_itviec_jobs",
        
        # Tầng Staging & Intermediate (dbt tạo ra)
        "stg_topcv_jobs",
        "stg_itviec_jobs",
        "int_all_jobs",
        
        # Tầng Silver (AI xử lý)
        "silver_all_jobs",
        
        # Tầng Gold (dbt tổng hợp)
        "gold_role_summary",
        "gold_tech_stack_by_level",
        "gold_tech_stack_counts"
    ]

    for table in tables_to_drop:
        conn.execute(f"DROP TABLE IF EXISTS {table};")
        print(f" [OK] Đã xóa bảng: {table}")

except Exception as e:
    print(f"\n[!] Lỗi khi tương tác với database: {e}")
finally:
    conn.close()
    print("\n✅ DỌN DẸP HOÀN TẤT! DuckDB của bạn đã sạch sẽ như mới.")