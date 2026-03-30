from curl_cffi import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time
import random
import glob

print("ACTIVATING PIPELINE 2: DATA ENRICHMENT...")

# 1. FIND AND READ THE LATEST CSV FILE FROM PIPELINE 1
csv_files = glob.glob('../data/raw/itviec_raw_*.csv')
if not csv_files:
    print("No raw files found! Please run the listing scraper script first.")
    exit()

latest_csv = max(csv_files, key=os.path.getctime)
print(f"Reading data from source: {latest_csv}")

df = pd.read_csv(latest_csv)
total_jobs = len(df)

# Check if 'job_description' column exists, if not, create an empty one
if 'job_description' not in df.columns:
    df['job_description'] = ''

df['job_description'] = df['job_description'].astype('object')

# Keep the anti-bot weapons (You can add your Cookie here to be 100% sure)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

print(f"Starting the deep dive process for {total_jobs} jobs...\n")

# 2. LOOP THROUGH EACH URL (Using Pandas iterrows)
for index, row in df.iterrows():
    job_url = row['job_url']
    
    # Skip rows with invalid URLs or those that already have a description (idempotent run)
    if pd.isna(job_url) or not str(job_url).startswith('http'):
        continue
    if pd.notna(row['job_description']) and str(row['job_description']).strip() != '':
        continue

    print(f"[{index + 1}/{total_jobs}] Extracting details for: {job_url.split('/')[-1][:40]}...")
    
    try:
        response = requests.get(job_url, headers=headers, impersonate="chrome110")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            
            # TACTIC: Find the section containing the 'job-content' class as inspected via F12
            job_content = soup.find("section", class_=lambda x: x and "job-content" in x)
            
            if job_content:
                # get_text(separator="\n", strip=True) joins paragraphs with newlines for clean formatting
                jd_text = job_content.get_text(separator="\n", strip=True)
                df.at[index, 'job_description'] = jd_text
            else:
                df.at[index, 'job_description'] = "JD content not found"
                
        else:
            print(f"  -> Access error: {response.status_code}")
            df.at[index, 'job_description'] = f"Error {response.status_code}"
            
    except Exception as e:
         print(f"  -> Network crash or error: {e}")
         df.at[index, 'job_description'] = "Connection error"

    # ANTI-BAN SHIELD (Mandatory since we are sending numerous requests)
    # Rest lightly for 1.5 to 3.5 seconds to avoid overloading the server
    time.sleep(random.uniform(1.5, 3.5)) 
    
    # ADVANCED SAFETY NET: Auto-save a draft every 50 jobs
    if (index + 1) % 50 == 0:
        temp_path = latest_csv.replace('itviec_raw', 'itviec_enriched_TEMP')
        df.to_csv(temp_path, index=False, encoding='utf-8')
        print(f"  [Auto-Save] Safely backed up to row {index + 1}...")

# 3. SAVE THE FINAL RESULT
output_path = latest_csv.replace('itviec_raw', 'itviec_enriched')
df.to_csv(output_path, index=False, encoding='utf-8')

print(f"\nDATA ENRICHMENT COMPLETE! The masterpiece has been saved at: {output_path}")