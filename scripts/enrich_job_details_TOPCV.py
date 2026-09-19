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

# 2. LOCATE PENDING RAW PARQUET FILES IN LANDING ZONE
raw_files = sorted(glob.glob(os.path.join(landing_dir, "topcv_raw_*.parquet")))

if not raw_files:
    print("-> No pending raw TopCV files found in Landing Zone. Skipping enrichment.")
    exit(0)

# Retrieve the latest raw batch
target_raw_file = raw_files[-1]
print(f"-> Target raw file found: {os.path.basename(target_raw_file)}")

df = pd.read_parquet(target_raw_file)
if df.empty:
    print("-> Raw file is empty. Exiting.")
    exit(0)

total_jobs = len(df)
print(f"-> Total jobs to enrich: {total_jobs}\n")

# 3. FLARESOLVERR CONFIGURATION
# In Docker network: http://flaresolverr:8191/v1
# On host machine: http://localhost:8191/v1
default_flaresolverr = "http://flaresolverr:8191/v1" if os.path.exists("/.dockerenv") else "http://localhost:8191/v1"
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", default_flaresolverr)
print(f"-> Connecting to FlareSolverr at: {FLARESOLVERR_URL}")

# 4. ENRICH JOB DESCRIPTIONS AND VERIFY STATUS
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
        "maxTimeout": 60000  # Allow up to 60s for Cloudflare challenge bypass
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

            # Check if still blocked by Cloudflare
            page_title = soup.title.text if soup.title else ""
            if "Access denied" in html_source or "Cloudflare" in page_title:
                print("   -> FlareSolverr returned a page, but it is still blocked.")
                jd_text = "Connection error"
            else:
                # Check for expiration signals on TopCV via DOM selectors
                # Element: .box-apply-expired or .apply-expired (contains 'Hết hạn ứng tuyển')
                is_expired = False
                if soup.select_one(".box-apply-expired, .apply-expired"):
                    is_expired = True
                else:
                    apply_group = soup.find("div", class_="box-group-button-apply")
                    if apply_group and "hết hạn ứng tuyển" in apply_group.get_text().lower():
                        is_expired = True

                if is_expired:
                    print("   -> [!] Job is EXPIRED on TopCV (Selector '.box-apply-expired / .apply-expired'). Marking as EXPIRED.")
                    jd_text = "EXPIRED"
                else:
                    # Extract detailed JD body
                    job_content = soup.find("div", id="box-job-information-detail")
                    if not job_content:
                        job_content = soup.find("div", class_="job-description__item") or \
                                      soup.find("div", class_="box-info-job")

                    if job_content:
                        # Cap at 3500 chars to optimize OpenAI token usage downstream
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

    # Polite delay between requests to avoid rate limiting
    time.sleep(random.uniform(2.0, 4.0))

# 5. WRITE ENRICHED DATA TO A NEW PARQUET FILE
df['job_description'] = enriched_descriptions

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
enriched_file = os.path.join(landing_dir, f"topcv_enriched_{timestamp}.parquet")

# Save Parquet file with snappy compression to preserve schema
df.to_parquet(enriched_file, index=False, compression="snappy")

# 6. CLEAN UP TEMPORARY RAW FILE
try:
    os.remove(target_raw_file)
except Exception as e:
    print(f"Warning: Could not remove temporary raw file: {e}")

print("=" * 70)
print(f"-> MISSION COMPLETED!")
print(f"   - Successfully extracted: {success_count}/{total_jobs} JDs")
print(f"   - Staged to Landing Zone: {enriched_file}")
print("=" * 70)