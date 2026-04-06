from curl_cffi import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time          
import random
import duckdb
from dotenv import load_dotenv

print("Starting the hunt on TopCV with heavy weapons...")

# 1. Download environment variables from the hidden .env file
load_dotenv()

# 2. Get Cookie from environment variables safely (Optional for TopCV, but good to have)
# Bạn có thể thêm TOPCV_COOKIE vào file .env nếu TopCV chặn quá gắt
topcv_cookie = os.getenv('TOPCV_COOKIE', '')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Referer': 'https://www.topcv.vn/',
    'Cookie': topcv_cookie 
}

keywords = [
    #'data-engineer', 
    'data-analyst', 
    'data-scientist',
    'analytics-engineer', 
    'machine-learning', 
    'ai-engineer',
    'business-intelligence'
]
jobs_data = []

for keyword in keywords:
    print(f"\n========== Hunting for: {keyword.upper()} jobs ==========")
    page = 1

    while True:
        print(f"---> Attacking {keyword.upper()} - Page {page}...")
        
        # URL động theo keyword của TopCV
        url = f"https://www.topcv.vn/tim-viec-lam-{keyword}?page={page}"
        
        try:
            # Add timeout=30 to report error if too slow, instead of hanging the system
            response = requests.get(url, headers=headers, impersonate="chrome110", timeout=30)
        except Exception as e:
            print(f"Network lag or blocked (Timeout) at page {page}: {e}")
            print("Taking a 10-second break to fool the server and try again...")
            time.sleep(10)
            continue # Skip the rest of this loop, go back to crawling this exact page again
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 🛑 LƯU Ý: Class của TopCV có thể thay đổi. Thường là 'job-item-2' hoặc chứa 'job-item'
            job_cards = soup.find_all("div", class_=lambda x: x and "job-item-search-result" in x.split() if x else False) 
            
            # THE "EMPTY PAGE" SKIP LOGIC: Fail Fast!
            if len(job_cards) == 0:
                print(f"[!] Page {page} is empty. No more jobs for {keyword.upper()}. Skipping to next keyword...")
                break 
                
            print(f"Captured {len(job_cards)} jobs.")
            
            # Extract data from each job card
            for card in job_cards:
                job_info = {
                    'job_title': '', 'company_name': '', 'location': '', 'salary_raw': '',
                    'tech_stack': '', 'job_url': '', 'source': 'TopCV',  # ĐỔI SOURCE THÀNH TOPCV
                    'crawl_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'experience_level': '', 
                    'job_category': keyword, 
                    'job_description': '' # Giữ trống để Pipeline 1.5 xử lý
                }
                
                # --- Get Job Title & Link URL ---
                try:
                    title_element = card.find("h3") or card.find("a", class_=lambda x: x and "title" in x)
                    job_info['job_title'] = title_element.text.strip()
                    
                    # Tìm link URL
                    link = title_element.get('href') if title_element.name == 'a' else card.find("a")['href']
                    if not link.startswith('http'):
                        link = "https://www.topcv.vn" + link
                    job_info['job_url'] = link
                except:
                    job_info['job_title'] = "Title extraction error"
                    job_info['job_url'] = f"error_url_{random.randint(1000,9999)}" # Tránh lỗi Primary Key rỗng

                # --- Get Company Name ---
                try:
                    company_element = card.find("a", class_=lambda x: x and "company" in x)
                    job_info['company_name'] = company_element.text.strip()
                except:
                    job_info['company_name'] = "Unknown company"

                # --- Get Location ---
                try:
                    # TopCV thường để location trong thẻ label có class 'address'
                    location_element = card.find("label", class_=lambda x: x and "address" in x)
                    job_info['location'] = location_element.text.strip() if location_element else "Unknown location"
                except:
                    job_info['location'] = "Unknown location"

                # --- Get Skills (Tech Stack) - Thường TopCV không hiện rõ ở list, ta để None trước ---
                try:
                    job_info['tech_stack'] = "To be extracted by AI"
                except:
                    job_info['tech_stack'] = "No tags"
                    
                # --- Get Salary ---
                try:
                    salary_element = card.find("label", class_=lambda x: x and "salary" in x)
                    job_info['salary_raw'] = salary_element.text.strip() if salary_element else "Thỏa thuận"
                except:
                    job_info['salary_raw'] = "Unknown"

                jobs_data.append(job_info)

        else:
            print(f"Blocked at page {page}! Error code: {response.status_code}")
            break # Break out of the inner while loop to try the next keyword
        
        # ANTI-BAN SHIELD
        # Sửa từ 3.5 - 6.2 thành 5.5 - 9.5
        sleep_time = random.uniform(5.5, 9.5)
        print(f"Resting for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

        page += 1
        
        # SAFETY NET: Just in case the website bugs out and loops infinitely
        if page > 50: 
            print(f"Reached 50 pages for {keyword.upper()}, applying emergency brake!")
            break

# 3. LOAD DATA INTO BRONZE LAYER (DUCKDB) WITH DUPLICATE PREVENTION
if jobs_data:
    print("\nLoading data into Database (Bronze Layer)...")
    
    # Connect to DuckDB
    db_path = '../job_market.duckdb' 
    conn = duckdb.connect(db_path)
    
    # 3.1. Ensure table raw_topcv_jobs exists (with PRIMARY KEY to prevent duplicates)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_topcv_jobs (
            job_url VARCHAR PRIMARY KEY,
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
    
    # 3.2. Insert data (Skip if job_url already exists)
    new_jobs_count = 0
    for job in jobs_data:
        try:
            conn.execute("""
                INSERT INTO raw_topcv_jobs (
                    job_url, job_title, company_name, location, salary_raw, 
                    tech_stack, source, crawl_timestamp, experience_level, 
                    job_category, job_description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (job_url) DO NOTHING;
            """, (
                job['job_url'], job['job_title'], job['company_name'], job['location'], 
                job['salary_raw'], job['tech_stack'], job['source'], job['crawl_timestamp'], 
                job['experience_level'], job['job_category'], job['job_description']
            ))
            
            new_jobs_count += 1 
            
        except Exception as e:
            # Bỏ qua lỗi duplicate in ra màn hình để terminal sạch sẽ
            if "Constraint Error" not in str(e):
                print(f"Error inserting job {job['job_title']}: {e}")
            
    # Statistics
    total_raw = conn.execute("SELECT COUNT(*) FROM raw_topcv_jobs").fetchone()[0]
    conn.close()
    
    print(f" MISSION ACCOMPLISHED!")
    print(f"   - Number of jobs collected this time: {len(jobs_data)}")
    print(f"   - Total number of jobs in Database (Bronze Layer - TopCV): {total_raw}")
else:
    print("\nMission failed: No data collected.")