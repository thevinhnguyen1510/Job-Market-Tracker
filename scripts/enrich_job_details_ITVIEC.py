import os
import glob
import time
import random
import pandas as pd
from curl_cffi import requests
from bs4 import BeautifulSoup
from datetime import datetime

print("ACTIVATING PIPELINE 1.5: DEEP DIVE INTO JOB DESCRIPTIONS (PARQUET EDITION)...")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
landing_dir = os.path.join(BASE_DIR, "data", "landing", "itviec")

# 1. Tìm các file raw parquet vừa cào
raw_files = sorted(glob.glob(os.path.join(landing_dir, "itviec_raw_*.parquet")))

if not raw_files:
    print("No pending raw ITViec files in Landing Zone. Skipping enrichment.")
    exit(0)

# Lấy file raw mới nhất
latest_file = raw_files[-1]
print(f"Reading jobs from: {latest_file}")
df = pd.read_parquet(latest_file)

if df.empty:
    print("File is empty. Exiting.")
    exit(0)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# 2. Cào Job Description
enriched_descriptions = []
total_jobs = len(df)

for index, row in df.iterrows():
    job_url = row['job_url']
    print(f"[{index + 1}/{total_jobs}] Extracting details for: {job_url.split('/')[-1][:40]}...")
    
    jd_text = ""
    try:
        response = requests.get(job_url, headers=headers, impersonate="chrome110", timeout=30)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            job_content = soup.find("section", class_=lambda x: x and "job-content" in x)
            jd_text = job_content.get_text(separator="\n", strip=True) if job_content else "JD content not found"
        else:
            jd_text = f"Error {response.status_code}"
    except Exception as e:
        jd_text = "Connection error"

    enriched_descriptions.append(jd_text)
    time.sleep(random.uniform(1.5, 3.0))

# 3. Cập nhật cột job_description vào DataFrame
df['job_description'] = enriched_descriptions

# 4. Lưu ra file enriched parquet
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
enriched_file = os.path.join(landing_dir, f"itviec_enriched_{timestamp}.parquet")
df.to_parquet(enriched_file, index=False, compression="snappy")

# Xóa file raw cũ sau khi đã enrich xong để tránh trùng
os.remove(latest_file)

print(f"\n[OK] Enriched data safely saved to: {enriched_file}")