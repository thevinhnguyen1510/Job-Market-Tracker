import duckdb

print("--- DUCKDB INSPECTION TOOL ---")

# Connect to your database
db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

# 1. CHECK TABLES: What tables exist in this database?
print("\n[1] TABLES IN DATABASE:")
print(conn.execute("SHOW TABLES;").df())

# 2. CHECK SCHEMA: What columns do we have, and what are their data types?
print("\n[2] SCHEMA OF 'silver_itviec_jobs':")
print(conn.execute("DESCRIBE silver_itviec_jobs;").df())

# 3. CHECK DATA: Let's peek at the actual AI-enriched data (First 3 rows)
print("\n[3] SAMPLE AI-ENRICHED DATA:")
sample_query = """
    SELECT ai_job_role, job_title
    FROM silver_itviec_jobs 
    LIMIT 10;
"""
print(conn.execute(sample_query).df())

# 4. CHECK PROGRESS
count_query = """
    SELECT COUNT(*) 
    FROM silver_itviec_jobs;
"""
processed_count = conn.execute(count_query).fetchone()[0]
print(f"\n[4] Total jobs in database: {processed_count}")

conn.close()
print("\n--- Inspection Complete ---")