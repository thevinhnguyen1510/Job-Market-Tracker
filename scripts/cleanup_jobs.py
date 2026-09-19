import os
import duckdb
from datetime import datetime

# ==============================================================================
# PIPELINE CLEANUP: TTL EXPIRATION BASED ON REAL CRAWL TIMESTAMPS
# ==============================================================================
EXPIRY_DAYS = 3

print("=" * 70)
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INITIATING CLEANUP JOBS...")
print("=" * 70)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

if not os.path.exists(db_path):
    print(f"Error: Database file not found at {db_path}")
    exit(1)

conn = duckdb.connect(db_path)

try:
    # 1. Count active jobs before cleanup
    active_before = conn.execute("""
        SELECT COUNT(*) FROM silver_all_jobs WHERE status = 'Active'
    """).fetchone()[0]
    print(f"-> Active jobs before cleanup: {active_before}")

    # 2. Resynchronize 'last_seen_at' with actual crawl timestamps from raw tables
    print("-> Synchronizing 'last_seen_at' with actual crawl timestamps from raw tables...")
    conn.execute("""
        UPDATE silver_all_jobs s
        SET last_seen_at = r.crawl_timestamp
        FROM int_all_jobs r
        WHERE s.job_id = r.job_id
          AND r.crawl_timestamp IS NOT NULL
          AND s.last_seen_at > r.crawl_timestamp
    """)

    # 3. Mark jobs as Inactive if not observed within EXPIRY_DAYS
    cleanup_query = f"""
        UPDATE silver_all_jobs
        SET status = 'Inactive'
        WHERE status = 'Active'
          AND last_seen_at < (CURRENT_TIMESTAMP - INTERVAL '{EXPIRY_DAYS} days')
    """
    conn.execute(cleanup_query)

    # 4. Aggregate cleanup statistics
    stats = conn.execute(f"""
        SELECT source, COUNT(*) as inactive_count
        FROM silver_all_jobs
        WHERE status = 'Inactive'
        GROUP BY source
    """).fetchall()

    active_after = conn.execute("""
        SELECT COUNT(*) FROM silver_all_jobs WHERE status = 'Active'
    """).fetchone()[0]

    deactivated_count = active_before - active_after
    print(f"\n[OK] CLEANUP SUMMARY:")
    print(f"   - Stale Jobs Deactivated this run: {deactivated_count}")
    print(f"   - Remaining Active Jobs in DB:     {active_after}")
    print(f"   - Total Inactive Jobs in DB:       {active_before - deactivated_count + (active_before - active_after)}")
    for source, cnt in stats:
        print(f"     * {source}: {cnt} inactive")
    print("=" * 70)

except Exception as e:
    print(f"[ERROR] Cleanup process failed: {e}")
    raise e
finally:
    conn.close()
    print("Cleanup task finished.")