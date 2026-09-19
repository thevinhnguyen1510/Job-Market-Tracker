import os
import time
import random
import duckdb
import requests as standard_requests
from curl_cffi import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ==============================================================================
# STEALTH DEEP PURGE: PACED VERIFICATION (ANTI-BOT & CLOUDFLARE BYPASS)
# ==============================================================================
print("=" * 70)
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INITIATING PACED DEEP PURGE...")
print("=" * 70)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

if not os.path.exists(db_path):
    print(f"Error: Database file not found at {db_path}")
    exit(1)

conn = duckdb.connect(db_path)
active_jobs = conn.execute("""
    SELECT job_id, job_url, source, job_title 
    FROM silver_all_jobs 
    WHERE status = 'Active'
    ORDER BY source, job_id
""").fetchall()
conn.close()

total_jobs = len(active_jobs)
print(f"-> Total Active jobs to verify: {total_jobs}")

# Kiểm tra FlareSolverr cho TopCV
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1")
has_flaresolverr = False
try:
    chk = standard_requests.get(FLARESOLVERR_URL, timeout=2)
    if chk.status_code in [200, 405]:
        has_flaresolverr = True
        print(f"-> FlareSolverr is ONLINE at {FLARESOLVERR_URL} (Will use to bypass TopCV Cloudflare)")
except Exception:
    print("-> FlareSolverr is offline. Will use direct curl_cffi for TopCV.")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Referer': 'https://google.com'
}

expired_job_ids = []
active_count = 0
error_count = 0

for index, job in enumerate(active_jobs, 1):
    job_id, job_url, source, title = job
    src = str(source).upper()
    short_title = title[:35] if title else job_url.split('/')[-1][:35]
    
    is_expired = False
    reason = "Active"

    try:
        # -------------------------------------------------------------
        # 1. KIỂM TRA TOPCV (Dùng FlareSolverr nếu có để vượt Cloudflare 403)
        # -------------------------------------------------------------
        if 'TOPCV' in src:
            html_text = ""
            if has_flaresolverr:
                try:
                    payload = {"cmd": "request.get", "url": job_url, "maxTimeout": 30000}
                    resp = standard_requests.post(FLARESOLVERR_URL, json=payload, timeout=35)
                    data = resp.json()
                    if data.get("status") == "ok":
                        html_text = data.get("solution", {}).get("response", "")
                except Exception:
                    pass
            
            # Fallback nếu FlareSolverr không trả về
            if not html_text:
                resp = requests.get(job_url, headers=headers, impersonate="chrome110", timeout=15)
                if resp.status_code in [404, 410]:
                    is_expired = True
                    reason = f"HTTP {resp.status_code}"
                else:
                    html_text = resp.text

            if html_text and not is_expired:
                soup = BeautifulSoup(html_text, "html.parser")
                if soup.select_one(".box-apply-expired, .apply-expired"):
                    is_expired = True
                    reason = "TopCV Box Apply Expired"
                else:
                    apply_group = soup.find("div", class_="box-group-button-apply")
                    if apply_group and "hết hạn ứng tuyển" in apply_group.get_text().lower():
                        is_expired = True
                        reason = "TopCV Apply Button Expired"
                    elif "tin tuyển dụng đã hết hạn" in html_text.lower():
                        is_expired = True
                        reason = "TopCV Expired Text"

        # -------------------------------------------------------------
        # 2. KIỂM TRA ITVIEC (Dùng curl_cffi giả lập Chrome)
        # -------------------------------------------------------------
        else:
            resp = requests.get(job_url, headers=headers, impersonate="chrome110", timeout=15)
            if resp.status_code in [404, 410]:
                is_expired = True
                reason = f"HTTP {resp.status_code}"
            elif resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                job_actions = soup.find("div", class_="job-actions")
                if job_actions:
                    if soup.select_one(".job-actions .bg-light-warning-color") or "expired" in job_actions.get_text().lower():
                        is_expired = True
                        reason = "ITViec Expired Badge"
                if not is_expired and "việc làm này đã hết hạn" in resp.text.lower():
                    is_expired = True
                    reason = "ITViec Expired Text"

    except Exception as e:
        error_count += 1
        reason = f"Network Timeout / Skip"

    # Kết quả từng job
    if is_expired:
        expired_job_ids.append(job_id)
        print(f"[{index}/{total_jobs}] [EXPIRED] -> {src} | {reason} | {short_title}")
    else:
        active_count += 1
        if index % 20 == 0 or index == total_jobs:
            print(f"[{index}/{total_jobs}] Checked... (Found {len(expired_job_ids)} expired so far)")

    # Sleep điều độ chống rate limit (tương tự crawler)
    time.sleep(random.uniform(0.6, 1.2))

print("\n" + "=" * 70)
print(f"SCAN COMPLETED:")
print(f"   - Total Checked:  {total_jobs}")
print(f"   - Still Active:   {active_count}")
print(f"   - EXPIRED Found:  {len(expired_job_ids)}")
print(f"   - Errors/Skipped: {error_count}")
print("=" * 70)

# Cập nhật ngay vào DuckDB nếu tìm thấy
if expired_job_ids:
    print(f"\nUpdating {len(expired_job_ids)} expired jobs to 'Inactive' in DuckDB...")
    conn = duckdb.connect(db_path)
    try:
        BATCH_SIZE = 500
        for i in range(0, len(expired_job_ids), BATCH_SIZE):
            batch = expired_job_ids[i:i + BATCH_SIZE]
            placeholders = ", ".join(["?"] * len(batch))
            conn.execute(f"""
                UPDATE silver_all_jobs 
                SET status = 'Inactive'
                WHERE job_id IN ({placeholders})
            """, batch)
        print("   [OK] DuckDB updated successfully!")
    finally:
        conn.close()

    print("\nSynchronizing Qdrant Vector DB (Purging Inactive vectors)...")
    os.system("python scripts/sync_qdrant.py")
else:
    print("\nAll active jobs are verified alive!")

print("\nDone.")
