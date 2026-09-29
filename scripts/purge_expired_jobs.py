import os
import sys
import time
import random
import duckdb
import requests as standard_requests
from curl_cffi import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ==============================================================================
# STEALTH DEEP PURGE: ADAPTIVE VERIFICATION (ANTI-BOT & CLOUDFLARE BYPASS)
# ==============================================================================
print("=" * 70)
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INITIATING ADAPTIVE DEEP PURGE...")
print("=" * 70)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

if not os.path.exists(db_path):
    print(f"Error: Database file not found at {db_path}")
    sys.exit(1)

conn = duckdb.connect(db_path)
total_active = conn.execute("SELECT COUNT(*) FROM silver_jobs WHERE status = 'Active'").fetchone()[0]

# Adaptive Verification: Chỉ quét các job có nguy cơ hết hạn cao (days_open >= 7)
# Job mới đăng (< 7 ngày) 99% vẫn đang tuyển, bỏ qua để tiết kiệm 70% request & tránh bị ban IP.
active_jobs = conn.execute("""
    SELECT job_id, job_url, source, job_title, days_open
    FROM silver_jobs 
    WHERE status = 'Active'
      AND days_open >= 7
    ORDER BY days_open DESC, source, job_id
""").fetchall()
conn.close()

total_jobs = len(active_jobs)
print(f"-> Total Active jobs in DB: {total_active}")
print(f"-> High-risk jobs to verify (days_open >= 7 days): {total_jobs}")
print(f"-> Fresh jobs skipped (< 7 days): {total_active - total_jobs}\n")

if total_jobs == 0:
    print("All active jobs are fresh (< 7 days). No verification needed.")
    sys.exit(0)

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
    job_id, job_url, source, title, days_open = job
    src = str(source).upper()
    short_title = title[:35] if title else job_url.split('/')[-1][:35]
    is_expired = False
    reason = ""

    try:
        # -------------------------------------------------------------
        # 1. KIỂM TRA TOPCV (Ưu tiên FlareSolverr nếu có)
        # -------------------------------------------------------------
        if src == 'TOPCV':
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
        reason = f"Network Timeout / Skip: {e}"

    # Kết quả từng job
    if is_expired:
        expired_job_ids.append(job_id)
        print(f"[{index}/{total_jobs}] [EXPIRED] -> {src} | {days_open}d open | {reason} | {short_title}")
    else:
        active_count += 1
        if index % 20 == 0 or index == total_jobs:
            print(f"[{index}/{total_jobs}] Checked... (Found {len(expired_job_ids)} expired so far)")

    # Sleep điều độ chống rate limit
    time.sleep(random.uniform(0.6, 1.2))

print("\n" + "=" * 70)
print(f"SCAN COMPLETED:")
print(f"   - Total High-Risk Checked: {total_jobs}")
print(f"   - Still Active:            {active_count}")
print(f"   - EXPIRED Found:           {len(expired_job_ids)}")
print(f"   - Errors/Skipped:          {error_count}")
print("=" * 70)

# Cập nhật ngay vào Bronze DuckDB nếu tìm thấy
if expired_job_ids:
    print(f"\nPersisting {len(expired_job_ids)} expired jobs into DuckDB Bronze tables...")
    conn = duckdb.connect(db_path)
    try:
        BATCH_SIZE = 500
        for i in range(0, len(expired_job_ids), BATCH_SIZE):
            batch = expired_job_ids[i:i + BATCH_SIZE]
            placeholders = ", ".join(["?"] * len(batch))
            conn.execute(f"""
                UPDATE raw_itviec_jobs 
                SET job_description = 'EXPIRED'
                WHERE job_id IN ({placeholders})
            """, batch)
            conn.execute(f"""
                UPDATE raw_topcv_jobs 
                SET job_description = 'EXPIRED'
                WHERE job_id IN ({placeholders})
            """, batch)
        print("   [OK] Bronze tables updated successfully with EXPIRED flag!")
    finally:
        conn.close()

    print("\nSynchronizing Qdrant Vector DB (Soft Delete)...")
    python_cmd = sys.executable
    os.system(f'"{python_cmd}" scripts/sync_qdrant.py')

    print("\nRebuilding dbt Silver and Gold models...")
    dbt_exe = os.path.join(BASE_DIR, "venv", "Scripts", "dbt.exe")
    dbt_dir = os.path.join(BASE_DIR, "analytics_dbt")
    if os.path.exists(dbt_exe):
        os.system(f'cd "{dbt_dir}" && "{dbt_exe}" run')
    else:
        os.system(f'cd "{dbt_dir}" && dbt run')
else:
    print("\nAll scanned high-risk jobs are verified alive!")

print("\nDone.")
