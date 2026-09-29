import os
import sys
import duckdb
from datetime import datetime

# ==============================================================================
# PIPELINE STATUS REPORT & AUDIT: SILVER JOBS METRICS
# ==============================================================================
print("=" * 70)
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] AUDITING SILVER JOBS STATUS...")
print("=" * 70)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

if not os.path.exists(db_path):
    print(f"Error: Database file not found at {db_path}")
    sys.exit(1)

conn = duckdb.connect(db_path, read_only=True)

try:
    # 1. Total jobs count
    total_jobs = conn.execute("SELECT COUNT(*) FROM silver_jobs").fetchone()[0]
    active_count = conn.execute("SELECT COUNT(*) FROM silver_jobs WHERE status = 'Active'").fetchone()[0]
    inactive_count = conn.execute("SELECT COUNT(*) FROM silver_jobs WHERE status = 'Inactive'").fetchone()[0]

    # 2. Stats by source
    stats_by_source = conn.execute("""
        SELECT 
            source,
            COUNT(*) FILTER (WHERE status = 'Active') AS active_jobs,
            COUNT(*) FILTER (WHERE status = 'Inactive') AS inactive_jobs,
            ROUND(AVG(days_open) FILTER (WHERE status = 'Active'), 1) AS avg_active_days_open
        FROM silver_jobs
        GROUP BY source
    """).fetchall()

    print(f"-> Total Jobs in Silver Layer: {total_jobs}")
    print(f"   * Active Jobs:             {active_count} ({active_count/total_jobs*100:.1f}%)")
    print(f"   * Inactive / Expired Jobs: {inactive_count} ({inactive_count/total_jobs*100:.1f}%)\n")

    print("-> Breakdown by Source:")
    for src, act, inact, avg_days in stats_by_source:
        print(f"   * [{src}] Active: {act} | Inactive: {inact} | Avg Active Days Open: {avg_days} days")

    # 3. High risk of expiration (Active >= 7 days)
    high_risk_count = conn.execute("""
        SELECT COUNT(*) FROM silver_jobs WHERE status = 'Active' AND days_open >= 7
    """).fetchone()[0]
    print(f"\n-> High-Risk Jobs (Active >= 7 days, eligible for Deep Purge): {high_risk_count}")

finally:
    conn.close()

print("=" * 70)
print("AUDIT COMPLETED.")