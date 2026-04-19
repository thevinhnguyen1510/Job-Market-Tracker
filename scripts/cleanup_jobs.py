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
        conn.execute("""
            UPDATE silver_all_jobs
            SET status = 'Inactive'
            WHERE source = 'ITVIEC' 
              AND job_url NOT IN (SELECT job_url FROM raw_itviec_jobs)
        """)
        itviec_cleaned = conn.execute("SELECT changes()").fetchone()[0]
        print(f"   [OK] Marked {itviec_cleaned} ITVIEC jobs as Inactive.")
    except duckdb.CatalogException:
        print("   [SKIP] Table raw_itviec_jobs does not exist yet.")

    # --- 2. CLEANING TOPCV ---
    print("-> Checking TOPCV jobs...")
    try:
        conn.execute("""
            UPDATE silver_all_jobs
            SET status = 'Inactive'
            WHERE source = 'TOPCV' 
              AND job_url NOT IN (SELECT job_url FROM raw_topcv_jobs)
        """)
        topcv_cleaned = conn.execute("SELECT changes()").fetchone()[0]
        print(f"   [OK] Marked {topcv_cleaned} TOPCV jobs as Inactive.")
    except duckdb.CatalogException:
        print("   [SKIP] Table raw_topcv_jobs does not exist yet.")

    print("\nAll expired jobs successfully hidden.")

except Exception as e:
    print(f"System Error during cleanup: {e}")
finally:
    if 'conn' in locals():
        conn.close()