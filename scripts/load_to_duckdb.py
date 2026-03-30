import duckdb
import os
import glob

print("Initializing mini Data Warehouse with DuckDB...")

# 1. Find the latest CSV file you just scraped (Targeting the enriched file)
csv_files = glob.glob('../data/raw/itviec_enriched_*.csv')

# Safety check: Fallback to raw files if enriched files are not found
if not csv_files:
    print("No enriched CSV files found! Looking for raw files instead...")
    csv_files = glob.glob('../data/raw/itviec_raw_*.csv')
    if not csv_files:
        print("No CSV files found at all. Please run the scraper first.")
        exit()

latest_csv = max(csv_files, key=os.path.getctime)
print(f"Found data file: {latest_csv}")

# 2. Create (or connect to) the DuckDB database file (located in the root folder)
db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

# 3. USE SQL TO READ CSV AND CREATE RAW TABLE
# DuckDB's read_csv_auto function is extremely smart; it automatically infers data types
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS raw_itviec_jobs AS 
    SELECT * FROM read_csv_auto('{latest_csv}');
"""

# If the table already exists from a previous run, drop it to ensure fresh data (for testing purposes)
conn.execute("DROP TABLE IF EXISTS raw_itviec_jobs;")
conn.execute(create_table_query)

# 4. Verify the result with a basic SQL SELECT statement
result = conn.execute("SELECT COUNT(*) FROM raw_itviec_jobs").fetchone()
print(f"Success! The 'raw_itviec_jobs' table in DuckDB currently has {result[0]} rows.")

# Print the first 3 rows to verify the data
print("\n--- First 3 rows in the Database ---")
print(conn.execute("SELECT job_title, company_name, location FROM raw_itviec_jobs LIMIT 3").df())

print("\n--- Table Schema in DuckDB ---")
print(conn.execute("DESCRIBE raw_itviec_jobs;").df())

conn.close()