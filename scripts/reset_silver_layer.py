import os
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

if not os.path.exists(db_path):
    db_path = 'job_market.duckdb'

conn = duckdb.connect(db_path)

try:
    print("Resetting Silver Layer tables...")
    conn.execute("DROP TABLE IF EXISTS silver_jobs;")
    conn.execute("DROP TABLE IF EXISTS silver_job_skills;")
    conn.execute("DROP VIEW IF EXISTS silver_all_jobs;")
    conn.execute("VACUUM;")
    print("Silver tables dropped successfully. Run 'dbt run --select silver' to re-create.")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()