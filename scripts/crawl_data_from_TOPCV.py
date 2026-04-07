# ==========================================
# 1. IMPORTS
# ==========================================
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time          
import random
import duckdb
from dotenv import load_dotenv

print("Starting the hunt on TopCV with heavy weapons (Cloudflare & Trap Bypass)...")

# 1. Download environment variables from the hidden .env file
load_dotenv()

# Biến cookie và headers giữ nguyên theo code cũ của bạn
topcv_cookie = os.getenv('TOPCV_COOKIE', '')
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Referer': 'https://www.topcv.vn/',
    'Cookie': topcv_cookie 
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

# ==========================================
# 2. CREATE CHROME DRIVER TO BYPASS 403
# ==========================================
options = uc.ChromeOptions()
# ÉP DÙNG CHROME 146 ĐỂ TRÁNH LỖI PHIÊN BẢN
driver = uc.Chrome(options=options, version_main=146)

try:
    for keyword in keywords:
        print(f"\n========== Hunting for: {keyword.upper()} jobs ==========")
        page = 1
        
        # BỘ NHỚ TẠM: Lưu các link đã cào để chống bẫy lặp lại của TopCV
        seen_urls_this_keyword = set()

        while True:
            print(f"---> Attacking {keyword.upper()} - Page {page}...")
            
            # URL dynamically based on keyword of TopCV
            url = f"https://www.topcv.vn/tim-viec-lam-{keyword}?page={page}"
            
            # ----------------------------------------------------
            # START REGION: BYPASS 403 CLOUDFLARE
            # ----------------------------------------------------
            try:
                # Use real Chrome to open URL instead of requests
                driver.get(url)
                
                # Wait a few seconds for Cloudflare to complete the security check
                time.sleep(random.uniform(4.5, 6.5))
                
                # Get HTML source after web has finished loading
                html_source = driver.page_source
                soup = BeautifulSoup(html_source, "html.parser")
                
                # Check if there's a Captcha/Block
                if "Access denied" in html_source or "Cloudflare" in soup.title.text:
                    print(f"Network lag or blocked (Cloudflare) at page {page}...")
                    print("Taking a 15-second break to fool the server and try again...")
                    time.sleep(15)
                    continue
                    
            except Exception as e:
                print(f"Network lag or blocked (Timeout) at page {page}: {e}")
                print("Taking a 10-second break to fool the server and try again...")
                time.sleep(10)
                continue 
            # ----------------------------------------------------
            # END REGION: BYPASS 403 CLOUDFLARE
            # ----------------------------------------------------
            
            # Find Job Cards
            job_cards = soup.find_all("div", class_=lambda x: x and "job-item-search-result" in x.split() if x else False) 
            
            # THE "EMPTY PAGE" SKIP LOGIC: Fail Fast!
            if len(job_cards) == 0:
                print(f"[!] Page {page} is empty. No more jobs for {keyword.upper()}. Skipping to next keyword...")
                break 
                
            print(f"Found {len(job_cards)} job cards on page. Filtering duplicates...")
            
            # Đếm số job THỰC SỰ MỚI trên trang này
            new_jobs_on_page = 0
            
            # Extract data from each job card
            for card in job_cards:
                # --- Get Job Title & Link URL ---
                try:
                    title_element = card.find("h3") or card.find("a", class_=lambda x: x and "title" in x)
                    job_title = title_element.text.strip()
                    
                    # Get link URL
                    link = title_element.get('href') if title_element.name == 'a' else card.find("a")['href']
                    if not link.startswith('http'):
                        link = "https://www.topcv.vn" + link
                        
                    # 👑 BÍ KÍP MỚI: Chặt đứt đuôi theo dõi (Tracking Parameters)
                    core_link = link.split('?')[0] 
                    
                except:
                    job_title = "Title extraction error"
                    core_link = f"error_url_{random.randint(1000,9999)}"

                # KIỂM TRA TRÙNG LẶP DỰA TRÊN CORE LINK (LINK GỐC KHÔNG CÓ ĐUÔI)
                if core_link in seen_urls_this_keyword:
                    continue
                
                # Nếu là link mới -> Lưu vào bộ nhớ và tiếp tục xử lý
                seen_urls_this_keyword.add(core_link)
                new_jobs_on_page += 1

                # ⚠️ SỬA LẠI VALUE CỦA job_url THÀNH core_link
                job_info = {
                    'job_title': job_title, 'company_name': '', 'location': '', 'salary_raw': '',
                    'tech_stack': 'To be extracted by AI', 'job_url': core_link, 'source': 'TopCV',
                    'crawl_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'experience_level': '', 
                    'job_category': keyword, 
                    'job_description': ''
                }

                # --- Get Company Name ---
                try:
                    company_element = card.find("a", class_=lambda x: x and "company" in x)
                    job_info['company_name'] = company_element.text.strip()
                except:
                    job_info['company_name'] = "Unknown company"

                # --- Get Location ---
                try:
                    location_element = card.find("label", class_=lambda x: x and "address" in x)
                    job_info['location'] = location_element.text.strip() if location_element else "Unknown location"
                except:
                    job_info['location'] = "Unknown location"
                    
                # --- Get Salary ---
                try:
                    salary_element = card.find("label", class_=lambda x: x and "salary" in x)
                    job_info['salary_raw'] = salary_element.text.strip() if salary_element else "Thỏa thuận"
                except:
                    job_info['salary_raw'] = "Unknown"

                jobs_data.append(job_info)

            # LOGIC DỪNG CUỘC CHƠI (BẪY PHÂN TRANG)
            print(f"Captured {new_jobs_on_page} REAL jobs.")
            if new_jobs_on_page == 0:
                print(f"[!] Toàn bộ job trên trang {page} đều là lặp lại (Gợi ý). Đã hết kết quả xịn cho {keyword.upper()}!")
                break # Ngắt luôn vòng lặp, chuyển sang từ khóa tiếp theo

            # ANTI-BAN SHIELD
            sleep_time = random.uniform(5.5, 9.5)
            print(f"Resting for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

            page += 1
            
            # SAFETY NET: Just in case the website bugs out and loops infinitely
            if page > 50: 
                print(f"Reached 50 pages for {keyword.upper()}, applying emergency brake!")
                break

finally:
    # Bắt buộc đóng trình duyệt khi chạy xong
    print("\nClosing Chrome driver...")
    driver.quit()


# ==========================================
# 3. LOAD DATA INTO BRONZE LAYER (DUCKDB) WITH DUPLICATE PREVENTION
# ==========================================
if jobs_data:
    print("\nLoading data into Database (Bronze Layer)...")
    
    # Ép đường dẫn cố định về file DB ở thư mục gốc để tránh đẻ file lung tung
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
    conn = duckdb.connect(db_path)
    
    # Ensure table raw_topcv_jobs exists
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
    
    # Insert data (Skip if job_url already exists)
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