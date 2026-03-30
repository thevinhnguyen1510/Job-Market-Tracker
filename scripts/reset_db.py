import duckdb

print("Resetting all AI enrichment data...")
conn = duckdb.connect('../job_market.duckdb') # Make sure the path is correct

# Clear all AI-filled columns
conn.execute("""
    UPDATE raw_itviec_jobs 
    SET min_years_of_experience = NULL, 
        ai_core_tech_stack = NULL, 
        english_requirement = NULL, 
        job_level = NULL;
""")

# Check how many rows are back to pending state
pending = conn.execute("SELECT COUNT(*) FROM raw_itviec_jobs WHERE job_level IS NULL").fetchone()[0]
print(f"Cleaning complete! There are {pending} jobs pending processing.")

conn.close()