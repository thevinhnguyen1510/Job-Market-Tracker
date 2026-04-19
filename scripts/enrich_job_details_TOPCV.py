# ==========================================
# PIPELINE 1.5: DEEP DIVE INTO JOB DESCRIPTIONS (FLARESOLVERR API EDITION)
# ==========================================
import requests
from bs4 import BeautifulSoup
import time
import random
import os
import duckdb
from dotenv import load_dotenv

print("ACTIVATING TOPCV PIPELINE 1.5: FLARESOLVERR EDITION...")

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
conn = duckdb.connect(db_path)

pending_jobs = conn.execute("""
    SELECT job_id, job_url 
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
# FLARESOLVERR CONFIGURATION
# ==========================================
# Choose the correct URL based on your Docker setup:
# If FlareSolverr is in the same docker-compose network: 'http://flaresolverr:8191/v1'
# If FlareSolverr is standalone and you use Docker Desktop: 'http://host.docker.internal:8191/v1'
FLARESOLVERR_URL = "http://flaresolverr:8191/v1" 

try:
    for index, (job_id, job_url) in enumerate(pending_jobs):
        print(f"[{index + 1}/{total_jobs}] Asking FlareSolverr to extract: {job_url.split('/')[-1][:40]}...")
        
        # FlareSolverr Request Payload
        payload = {
            "cmd": "request.get",
            "url": job_url,
            "maxTimeout": 60000 # Give FlareSolverr up to 60 seconds to bypass the challenge
        }
        
        jd_text = ""
        
        try:
            # Send the request to your local FlareSolverr server
            response = requests.post(
                FLARESOLVERR_URL, 
                headers={"Content-Type": "application/json"}, 
                json=payload
            )
            data = response.json()
            
            # Check if FlareSolverr successfully bypassed Cloudflare
            if data.get("status") == "ok":
                html_source = data.get("solution", {}).get("response", "")
                soup = BeautifulSoup(html_source, "html.parser")
                
                # Check for residual blocks just in case
                if "Access denied" in html_source or "Cloudflare" in soup.title.text:
                    print("   -> FlareSolverr returned a page, but it is still blocked.")
                    jd_text = "Connection error"
                else:
                    # Find JD Content
                    job_content = soup.find("div", id="box-job-information-detail")
                    if not job_content:
                        job_content = soup.find("div", class_="job-description__item") or \
                                      soup.find("div", class_="box-info-job")
                    
                    if job_content:
                        jd_text = job_content.get_text(separator="\n", strip=True)[:3500]
                        print("   -> Success! JD Extracted.")
                    else:
                        print("   -> Page loaded, but layout missing or changed.")
                        jd_text = "JD content not found"
            else:
                print(f"   -> FlareSolverr failed to solve: {data.get('message')}")
                jd_text = "Connection error"
                
        except requests.exceptions.RequestException as e:
            print(f"   -> Could not connect to FlareSolverr: {e}")
            print("   -> Make sure FlareSolverr is running and accessible at the specified URL.")
            jd_text = "Connection error"

        # Save to Database
        try:
            conn.execute("""
                UPDATE raw_topcv_jobs 
                SET job_description = ? 
                WHERE job_id = ?
            """, (jd_text, job_id))
        except Exception as e:
            print(f"   -> Error saving to Database: {e}")

        # Be polite to the server, even with FlareSolverr
        time.sleep(random.uniform(2.5, 4.5))

finally:
    print("\nClosing Database connection...")
    conn.close()
    
print("\nCompleted! All accessible TopCV Job Descriptions have been processed.")