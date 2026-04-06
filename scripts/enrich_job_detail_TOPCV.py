import duckdb
from curl_cffi import requests
from bs4 import BeautifulSoup
import time
import random
import os
from dotenv import load_dotenv

print("ACTIVATING TOPCV PIPELINE 1.5: DEEP DIVE INTO JOB DESCRIPTIONS...")

# 1. THÊM LOAD COOKIE ĐỂ VƯỢT CLOUDFLARE (Nếu bạn bị 403)
load_dotenv()
topcv_cookie = os.getenv('TOPCV_COOKIE', '')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Cookie': topcv_cookie
}

db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

# 2. ĐỔI TÊN BẢNG THÀNH RAW_TOPCV_JOBS
pending_jobs = conn.execute("""
    SELECT job_url 
    FROM raw_topcv_jobs 
    WHERE job_description IS NULL 
       OR job_description = '' 
       OR job_description = 'Connection error'
""").fetchall()

total_jobs = len(pending_jobs)
if total_jobs == 0:
    print("All TopCV jobs already have Job Description. No need to crawl.")
    conn.close()
    exit()

print(f"Found {total_jobs} TopCV jobs needing description extraction.\n")

for index, (job_url,) in enumerate(pending_jobs):
    print(f"[{index + 1}/{total_jobs}] Extracting details for: {job_url.split('/')[-1][:40]}...")
    
    jd_text = ""
    try:
        # Thêm timeout và impersonate
        response = requests.get(job_url, headers=headers, impersonate="chrome110", timeout=15)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 3. SỬA ĐOẠN TÌM KIẾM DỰA TRÊN ẢNH CỦA BẠN (DÙNG ID)
            job_content = soup.find("div", id="box-job-information-detail")
            
            if job_content:
                jd_text = job_content.get_text(separator="\n", strip=True)
                # Tối ưu: Cắt bớt phần text quá dài để tiết kiệm tiền API khi chạy file ai_extractor
                jd_text = jd_text[:3500] 
            else:
                jd_text = "JD content not found"
                
        else:
            print(f"   -> Access error: {response.status_code}")
            jd_text = f"Error {response.status_code}"
            
    except Exception as e:
         print(f"   -> Network crash or error: {e}")
         jd_text = "Connection error"

    try:
        # 4. CẬP NHẬT VÀO BẢNG RAW_TOPCV_JOBS
        conn.execute("""
            UPDATE raw_topcv_jobs 
            SET job_description = ? 
            WHERE job_url = ?
        """, (jd_text, job_url))
    except Exception as e:
        print(f"-> Error saving to Database: {e}")

    # 5. TĂNG THỜI GIAN NGỦ ĐỂ TRÁNH BỊ 403 KHI CÀO JD CHI TIẾT
    time.sleep(random.uniform(3.5, 7.5)) 

print("\nCompleted! All TopCV Job Descriptions have been safely loaded into the Bronze Layer.")
conn.close()