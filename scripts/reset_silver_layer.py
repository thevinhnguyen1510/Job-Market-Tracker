import duckdb

db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

try:
    # 1. DELETE OLD SILVER'S TABLE
    conn.execute("DROP TABLE IF EXISTS silver_itviec_jobs;")
    conn.execute("VACUUM;")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()