import os
import duckdb

print("INITIATING: GLOBAL CLEANUP (MARKING EXPIRED JOBS AS INACTIVE)...")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
conn = duckdb.connect(db_path)

try:
    # --- 1. CLEANING ITVIEC ---
    print("-> Checking ITVIEC jobs...")
    try:
        # DuckDB tự động trả về số dòng được update ngay trong kết quả lệnh
        itviec_cleaned = conn.execute("""
            UPDATE silver_all_jobs
            SET status = 'Inactive'
            WHERE source = 'ITVIEC' 
              AND job_url NOT IN (SELECT job_url FROM raw_itviec_jobs)
        """).fetchone()[0]
        
        print(f"   [OK] Marked {itviec_cleaned} ITVIEC jobs as Inactive.")
    except duckdb.CatalogException as e:
        print(f"   [SKIP] Catalog Error: {e}")

    # --- 2. CLEANING TOPCV ---
    print("-> Checking TOPCV jobs...")
    try:
        topcv_cleaned = conn.execute("""
            UPDATE silver_all_jobs
            SET status = 'Inactive'
            WHERE source = 'TOPCV' 
              AND job_url NOT IN (SELECT job_url FROM raw_topcv_jobs)
        """).fetchone()[0]
        
        print(f"   [OK] Marked {topcv_cleaned} TOPCV jobs as Inactive.")
    except duckdb.CatalogException as e:
        print(f"   [SKIP] Catalog Error: {e}")

    print("\nAll accessible jobs processed.")

except Exception as e:
    print(f"System Error during cleanup: {e}")
finally:
    if 'conn' in locals():
        conn.close()