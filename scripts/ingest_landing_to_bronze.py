import os
import glob
import shutil
import duckdb
from datetime import datetime

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INITIATING BRONZE INGESTION...")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
landing_base = os.path.join(BASE_DIR, 'data', 'landing')
archive_base = os.path.join(BASE_DIR, 'data', 'archive')

conn = duckdb.connect(db_path)

conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_itviec_jobs (
        job_id VARCHAR PRIMARY KEY, job_url VARCHAR, job_title VARCHAR,
        company_name VARCHAR, location VARCHAR, salary_raw VARCHAR,
        tech_stack VARCHAR, source VARCHAR, crawl_timestamp TIMESTAMP,
        experience_level VARCHAR, job_category VARCHAR, job_description VARCHAR
    );
    CREATE TABLE IF NOT EXISTS raw_topcv_jobs (
        job_id VARCHAR PRIMARY KEY, job_url VARCHAR, job_title VARCHAR,
        company_name VARCHAR, location VARCHAR, salary_raw VARCHAR,
        tech_stack VARCHAR, source VARCHAR, crawl_timestamp TIMESTAMP,
        experience_level VARCHAR, job_category VARCHAR, job_description VARCHAR
    );
""")

try:
    for source in ['itviec', 'topcv']:
        table_name = f"raw_{source}_jobs"
        source_landing_dir = os.path.join(landing_base, source)
        source_archive_dir = os.path.join(archive_base, source)
        os.makedirs(source_archive_dir, exist_ok=True)

        # Tìm các file đã enrich sẵn sàng nạp
        pattern = os.path.join(source_landing_dir, f"{source}_enriched_*.parquet")
        matching_files = glob.glob(pattern)

        if not matching_files:
            print(f"-> No enriched files found for {source.upper()}. Skipping.")
            continue

        print(f"-> Ingesting {len(matching_files)} file(s) into {table_name}...")
        
        # Đường dẫn dạng POSIX cho DuckDB đọc
        posix_pattern = pattern.replace("\\", "/")

        # Bulk upsert siêu tốc từ Parquet
        conn.execute(f"""
            INSERT INTO {table_name} (
                job_id, job_url, job_title, company_name, location,
                salary_raw, tech_stack, source, crawl_timestamp,
                experience_level, job_category, job_description
            )
            SELECT 
                job_id, job_url, job_title, company_name, location,
                salary_raw, tech_stack, source, crawl_timestamp,
                experience_level, job_category, job_description
            FROM read_parquet('{posix_pattern}')
            ON CONFLICT (job_id) DO UPDATE SET
                crawl_timestamp = EXCLUDED.crawl_timestamp,
                job_description = CASE 
                    WHEN EXCLUDED.job_description != '' AND EXCLUDED.job_description != 'Connection error' 
                    THEN EXCLUDED.job_description 
                    ELSE {table_name}.job_description 
                END;
        """)

        # Di chuyển các file đã nạp xong sang archive để lưu vết
        for f in matching_files:
            shutil.move(f, os.path.join(source_archive_dir, os.path.basename(f)))
            
        print(f"   [OK] Ingestion for {source.upper()} completed and files archived.")

    total_itviec = conn.execute("SELECT COUNT(*) FROM raw_itviec_jobs").fetchone()[0]
    total_topcv = conn.execute("SELECT COUNT(*) FROM raw_topcv_jobs").fetchone()[0]
    print(f"\nTotal in DB -> ITViec: {total_itviec} | TopCV: {total_topcv}")

finally:
    conn.close()
    print("Bronze Ingestion Finished. DB lock released.")