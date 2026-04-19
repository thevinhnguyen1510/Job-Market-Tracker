import duckdb

print("Begin running Ghost Task: Clean all Job expired...")

# 1. Connect to DB
db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

try:
    # 2. Count the number of Active jobs before cleaning
    active_before = conn.execute("SELECT COUNT(*) FROM silver_all_jobs WHERE status = 'Active'").fetchone()[0]
    print(f"📊 Number of active jobs: {active_before}")

    # 3. Execute the SQL command to clean up
    conn.execute("""
        UPDATE silver_all_jobs 
        SET status = 'Expired'
        WHERE status = 'Active' 
          AND last_seen_at < CURRENT_TIMESTAMP - INTERVAL '3 days';
    """)

    # 4. Check the results again
    active_after = conn.execute("SELECT COUNT(*) FROM silver_all_jobs WHERE status = 'Active'").fetchone()[0]
    expired_count = active_before - active_after
    
    print(f"Done scanning! There are {expired_count} old jobs that have been marked as Expired.")
    print(f"Current number of active jobs: {active_after}")

except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
    print("Ghost Task has been completed!")