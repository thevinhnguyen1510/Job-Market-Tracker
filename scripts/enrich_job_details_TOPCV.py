# ==========================================
# PIPELINE 1.5: DEEP DIVE INTO JOB DESCRIPTIONS (CLOUDFLARE BYPASS)
# ==========================================
from seleniumbase import Driver
from bs4 import BeautifulSoup
import time
import random
import os
import duckdb
from dotenv import load_dotenv

print("ACTIVATING TOPCV PIPELINE 1.5: DEEP DIVE INTO JOB DESCRIPTIONS WITH SELENIUMBASE...")

load_dotenv()

# 1. CONNECT TO DATABASE WITH STANDARD PATH (Prevent blind path errors)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
conn = duckdb.connect(db_path)

# 2. CHECK JOBS NEED DESCRIPTION EXTRACTION
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

# ==========================================
# 3. INITIALIZE SELENIUMBASE TO BYPASS 403
# ==========================================
driver = Driver(
    uc=True,              # Enable Undetected Mode (Cloudflare bypass)
    headless=True,        # Run invisibly in Docker
    no_sandbox=True,      # Required for Linux/Docker environment
    browser="chrome"      # Use Chromium core
)

try:
    for index, (job_url,) in enumerate(pending_jobs):
        print(f"[{index + 1}/{total_jobs}] Extracting: {job_url.split('/')[-1][:40]}...")
        
        jd_text = ""
        try:
            # USE SELENIUMBASE TO ACCESS JD LINK AND BYPASS CLOUDFLARE
            driver.uc_open_with_reconnect(job_url, reconnect_time=5)
            
            # Wait for web to load and bypass Cloudflare Check
            time.sleep(random.uniform(4.0, 6.0))
            
            # Auto-click CAPTCHA if it appears
            driver.uc_gui_click_captcha()
            time.sleep(2)
            
            html_source = driver.page_source
            soup = BeautifulSoup(html_source, "html.parser")
            
            # Check if blocked by Captcha/Block
            if "Access denied" in html_source or "Cloudflare" in soup.title.text:
                print(f"   -> Blocked by Cloudflare! Resting 15s...")
                time.sleep(15)
                jd_text = "Connection error" # Mark for retry next time
            else:
                # 4. FIND JD CONTENT
                job_content = soup.find("div", id="box-job-information-detail")
                
                if job_content:
                    jd_text = job_content.get_text(separator="\n", strip=True)
                    # Cut short to save AI costs
                    jd_text = jd_text[:3500] 
                else:
                    print("   -> Detail layout missing or changed.")
                    jd_text = "JD content not found"
                
        except Exception as e:
            print(f"   -> Network crash or error: {e}")
            jd_text = "Connection error"

        # 5. SAVE TO DATABASE
        try:
            conn.execute("""
                UPDATE raw_topcv_jobs 
                SET job_description = ? 
                WHERE job_url = ?
            """, (jd_text, job_url))
        except Exception as e:
            print(f"-> Error saving to Database: {e}")

        # 6. ANTI-BAN SHIELD: Rest between jobs
        sleep_time = random.uniform(4.5, 8.5)
        time.sleep(sleep_time)

finally:
    # Close the browser after finishing
    print("\nClosing Chrome driver...")
    driver.quit()
    conn.close()
    
print("\nCompleted! All accessible TopCV Job Descriptions have been loaded into the Bronze Layer.")