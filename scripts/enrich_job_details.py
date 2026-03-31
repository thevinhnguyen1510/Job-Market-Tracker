import duckdb
from curl_cffi import requests
from bs4 import BeautifulSoup
import time
import random

print("ACTIVATING PIPELINE 1.5: DEEP DIVE INTO JOB DESCRIPTIONS...")

# 1. Connect to database and find jobs that need description
db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

# Get URLs where job_description is empty or not crawled yet
pending_jobs = conn.execute("""
    SELECT job_url 
    FROM raw_itviec_jobs 
    WHERE job_description IS NULL 
       OR job_description = '' 
       OR job_description = 'Connection error'
""").fetchall()

total_jobs = len(pending_jobs)
if total_jobs == 0:
    print("All jobs already have Job Description. No need to crawl.")
    conn.close()
    exit()

print(f"Found {total_jobs} jobs needing description extraction.\n")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# 2. LOOP TO CRAWL DETAILS AND UPDATE DIRECTLY TO DATABASE
for index, (job_url,) in enumerate(pending_jobs):
    print(f"[{index + 1}/{total_jobs}] Extracting details for: {job_url.split('/')[-1][:40]}...")
    
    jd_text = ""
    try:
        response = requests.get(job_url, headers=headers, impersonate="chrome110")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            job_content = soup.find("section", class_=lambda x: x and "job-content" in x)
            
            if job_content:
                jd_text = job_content.get_text(separator="\n", strip=True)
            else:
                jd_text = "JD content not found"
                
        else:
            print(f"   -> Access error: {response.status_code}")
            jd_text = f"Error {response.status_code}"
            
    except Exception as e:
         print(f"   -> Network crash or error: {e}")
         jd_text = "Connection error"

    # UPDATE BACK TO DUCKDB BASED ON JOB_URL
    try:
        conn.execute("""
            UPDATE raw_itviec_jobs 
            SET job_description = ? 
            WHERE job_url = ?
        """, (jd_text, job_url))
    except Exception as e:
        print(f"-> Error saving to Database: {e}")

    # ANTI-BAN SHIELD
    time.sleep(random.uniform(1.5, 3.5)) 

print("\nCompleted! All Job Descriptions have been safely loaded into the Bronze Layer.")
conn.close()