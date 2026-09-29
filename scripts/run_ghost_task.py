import os
import sys
import duckdb

print("Running Ghost Task: Audit and Refresh Silver Layer TTL...")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

if not os.path.exists(db_path):
    db_path = 'job_market.duckdb'

conn = duckdb.connect(db_path, read_only=True)
try:
    active_jobs = conn.execute("SELECT COUNT(*) FROM silver_jobs WHERE status = 'Active'").fetchone()[0]
    inactive_jobs = conn.execute("SELECT COUNT(*) FROM silver_jobs WHERE status = 'Inactive'").fetchone()[0]
    print(f"Current Status: Active={active_jobs} | Inactive={inactive_jobs}")
finally:
    conn.close()

print("Ghost Task completed.")