from curl_cffi import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time          
import random
import duckdb
from dotenv import load_dotenv

print("Starting the hunt on ITviec with heavy weapons...")

# 1. Download environment variables from the hidden .env file
load_dotenv()

# 2. Get Cookie from environment variables safely
itviec_cookie = os.getenv('ITVIEC_COOKIE', '')

if not itviec_cookie:
    print("Cannot find ITVIEC_COOKIE configuration in the .env file!")
    print("Please create a .env file and add the cookie before running.")
    exit()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Referer': 'https://itviec.com/',
    'Cookie': itviec_cookie 
}

keywords = [
    'data-engineer', 
    'data-analyst', 
    'data-scientist',
    'analytics-engineer', 
    'machine-learning', 
    'ai-engineer',
    'business-intelligence'
]
jobs_data = []

global_seen_job_ids = set()

for keyword in keywords:
    print(f"\n========== Hunting for: {keyword.upper()} jobs ==========")
    page = 1

    while True:
        print(f"---> Attacking {keyword.upper()} - Page {page}...")
        
        # Make URL dynamic based on the current keyword in the loop
        url = f"https://itviec.com/it-jobs/{keyword}?page={page}"
        
        try:
            # Add timeout=30 to report error if too slow
            response = requests.get(url, headers=headers, impersonate="chrome110", timeout=30)
        except Exception as e:
            print(f"Network lag or blocked (Timeout) at page {page}: {e}")
            print("Taking a 10-second break to fool the server and try again...")
            time.sleep(10)
            continue 
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            job_cards = soup.find_all("div", class_="job-card") 
            
            # Fail Fast!
            if len(job_cards) == 0:
                print(f"[!] Page {page} is empty. No more jobs for {keyword.upper()}. Skipping to next keyword...")
                break 
                
            new_jobs_on_page = 0
            
            for card in job_cards:
                # --- Get Job Title & Link URL ---
                try:
                    title_element = card.find("h3") 
                    job_title = title_element.text.strip()
                    
                    # ITviec lấy link từ data-url
                    raw_url = title_element.get('data-url', '') 
                    if raw_url and not raw_url.startswith('http'):
                        raw_url = "https://itviec.com" + raw_url
                        
                    core_link = raw_url.split('?')[0]
                    
                    job_id = core_link
                    
                except:
                    job_title = "Title extraction error"
                    core_link = f"error_url_{random.randint(1000,9999)}"
                    job_id = core_link

                if job_id in global_seen_job_ids:
                    continue
                
                global_seen_job_ids.add(job_id)
                new_jobs_on_page += 1

                job_info = {
                    'job_id': job_id,
                    'job_title': job_title, 'company_name': '', 'location': '', 'salary_raw': '',
                    'tech_stack': '', 'job_url': core_link, 'source': 'ITViec', 
                    'crawl_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'experience_level': '', 
                    'job_category': keyword, 
                    'job_description': '' 
                }
                
                # --- Get Company Name ---
                try:
                    company_span = card.find("span", class_="text-hover-underline")
                    job_info['company_name'] = company_span.find("a").text.strip()
                except:
                    job_info['company_name'] = "Unknown company"

                # --- Get Location ---
                try:
                    location_element = card.find("div", class_="text-truncate", title=True)
                    job_info['location'] = location_element['title'] if location_element else "Unknown location"
                except:
                    job_info['location'] = "Unknown location"

                # --- Get Skills (Tech Stack) ---
                try:
                    tag_elements = card.find_all("a", class_=lambda x: x and "itag" in x.split())
                    tags = [tag.text.strip() for tag in tag_elements]
                    job_info['tech_stack'] = ", ".join(tags) 
                except:
                    job_info['tech_stack'] = "No tags"
                    
                # --- Get Salary ---
                try:
                    if card.find("a", class_="sign-in-view-salary"):
                        job_info['salary_raw'] = "Sign in to view salary"
                    else:
                        job_info['salary_raw'] = "Updating..." 
                except:
                    pass

                jobs_data.append(job_info)

            print(f"Captured {new_jobs_on_page} REAL jobs on page {page}.")
            if new_jobs_on_page == 0:
                print(f"[!] All the jobs on page {page} are duplicates. Breaking the loop for {keyword.upper()}!")
                break

        else:
            print(f"Blocked at page {page}! Error code: {response.status_code}")
            break 
        
        # ANTI-BAN SHIELD
        sleep_time = random.uniform(3.5, 6.2)
        print(f"Resting for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

        page += 1
        
        if page > 50: 
            print(f"Reached 50 pages for {keyword.upper()}, applying emergency brake!")
            break

# 3. LOAD DATA INTO BRONZE LAYER (DUCKDB) WITH DUPLICATE PREVENTION
if jobs_data:
    print("\nLoading data into Database (Bronze Layer)...")
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
    conn = duckdb.connect(db_path)
    
    # Ensure table raw_itviec_jobs exists (with job_id as PRIMARY KEY)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_itviec_jobs (
            job_id VARCHAR PRIMARY KEY,
            job_url VARCHAR,
            job_title VARCHAR,
            company_name VARCHAR,
            location VARCHAR,
            salary_raw VARCHAR,
            tech_stack VARCHAR,
            source VARCHAR,
            crawl_timestamp TIMESTAMP,
            experience_level VARCHAR,
            job_category VARCHAR,
            job_description VARCHAR
        );
    """)
    
    new_jobs_count = 0
    for job in jobs_data:
        try:
            conn.execute("""
                INSERT INTO raw_itviec_jobs (
                    job_id, job_url, job_title, company_name, location, salary_raw, 
                    tech_stack, source, crawl_timestamp, experience_level, 
                    job_category, job_description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (job_id) DO NOTHING;
            """, (
                job['job_id'], job['job_url'], job['job_title'], job['company_name'], job['location'], 
                job['salary_raw'], job['tech_stack'], job['source'], job['crawl_timestamp'], 
                job['experience_level'], job['job_category'], job['job_description']
            ))
            new_jobs_count += 1 
            
        except Exception as e:
            if "Constraint Error" not in str(e):
                print(f"Error inserting job {job['job_title']}: {e}")
            
    total_raw = conn.execute("SELECT COUNT(*) FROM raw_itviec_jobs").fetchone()[0]
    conn.close()
    
    print(f" MISSION ACCOMPLISHED!")
    print(f"   - Number of jobs collected this time: {len(jobs_data)}")
    print(f"   - Total number of jobs in Database (Bronze Layer): {total_raw}")
else:
    print("\nMission failed: No data collected.")