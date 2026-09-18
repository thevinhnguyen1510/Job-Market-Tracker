# ==============================================================================
# PIPELINE 1.5: DEEP DIVE INTO TOPCV JOB DESCRIPTIONS (FLARESOLVERR + PARQUET)
# ==============================================================================
import os
import glob
import time
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

print("=" * 70)
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ACTIVATING TOPCV ENRICHER (PARQUET EDITION)...")
print("=" * 70)

# 1. SETUP ENVIRONMENT & PATHS
load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
landing_dir = os.path.join(BASE_DIR, "data", "landing", "topcv")

# 2. KIỂM TRA VÀ TÌM FILE RAW PARQUET VỪA ĐƯỢC CÀO
raw_files = sorted(glob.glob(os.path.join(landing_dir, "topcv_raw_*.parquet")))

if not raw_files:
    print("-> No pending raw TopCV files found in Landing Zone. Skipping enrichment.")
    exit(0)

# Lấy file raw mới nhất được cào
target_raw_file = raw_files[-1]
print(f"-> Target raw file found: {os.path.basename(target_raw_file)}")

df = pd.read_parquet(target_raw_file)
if df.empty:
    print("-> Raw file is empty. Exiting.")
    exit(0)

total_jobs = len(df)
print(f"-> Total jobs to enrich: {total_jobs}\n")

# 3. FLARESOLVERR CONFIGURATION
# Nếu chạy trong Docker network: http://flaresolverr:8191/v1
# Nếu test trên máy host ngoài Docker: http://localhost:8191/v1
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://flaresolverr:8191/v1")
print(f"-> Connecting to FlareSolverr at: {FLARESOLVERR_URL}")

# 4. TIẾN HÀNH ENRICH TỪNG JOB
enriched_descriptions = []
success_count = 0

for index, row in df.iterrows():
    job_id = row.get('job_id', '')
    job_url = row.get('job_url', '')
    short_url = job_url.split('/')[-1][:45]

    print(f"[{index + 1}/{total_jobs}] Extracting: {short_url}...")

    payload = {
        "cmd": "request.get",
        "url": job_url,
        "maxTimeout": 60000  # Cho phép tối đa 60s để bypass Cloudflare
    }

    jd_text = ""

    try:
        response = requests.post(
            FLARESOLVERR_URL,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=70
        )
        data = response.json()

        if data.get("status") == "ok":
            html_source = data.get("solution", {}).get("response", "")
            soup = BeautifulSoup(html_source, "html.parser")

            # Kiểm tra xem có bị Cloudflare chặn tiếp không
            page_title = soup.title.text if soup.title else ""
            if "Access denied" in html_source or "Cloudflare" in page_title:
                print("   -> FlareSolverr returned a page, but it is still blocked.")
                jd_text = "Connection error"
            else:
                # Bóc tách nội dung chi tiết của JD
                job_content = soup.find("div", id="box-job-information-detail")
                if not job_content:
                    job_content = soup.find("div", class_="job-description__item") or \
                                  soup.find("div", class_="box-info-job")

                if job_content:
                    # Giới hạn 3500 ký tự để tối ưu token cho AI Extractor sau này
                    jd_text = job_content.get_text(separator="\n", strip=True)[:3500]
                    print("   -> [OK] Success! JD Extracted.")
                    success_count += 1
                else:
                    print("   -> [!] Page loaded, but JD element selector not matched.")
                    jd_text = "JD content not found"
        else:
            print(f"   -> FlareSolverr failed: {data.get('message')}")
            jd_text = "Connection error"

    except requests.exceptions.RequestException as e:
        print(f"   -> Could not connect to FlareSolverr: {e}")
        jd_text = "Connection error"

    enriched_descriptions.append(jd_text)

    # Nghỉ giữa các request để giảm tải và tránh bị chặn
    time.sleep(random.uniform(2.0, 4.0))

# 5. GHI DỮ LIỆU ĐÃ ENRICH RA FILE PARQUET MỚI
df['job_description'] = enriched_descriptions

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
enriched_file = os.path.join(landing_dir, f"topcv_enriched_{timestamp}.parquet")

# Lưu file Parquet (nén snappy, schema bảo toàn)
df.to_parquet(enriched_file, index=False, compression="snappy")

# 6. DỌN DẸP FILE RAW TẠM
try:
    os.remove(target_raw_file)
except Exception as e:
    print(f"Warning: Could not remove temporary raw file: {e}")

print("=" * 70)
print(f"-> MISSION COMPLETED!")
print(f"   - Successfully extracted: {success_count}/{total_jobs} JDs")
print(f"   - Staged to Landing Zone: {enriched_file}")
print("=" * 70)