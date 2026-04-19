# ==========================================
# 1. IMPORTS & CONFIGURATIONS
# ==========================================
import shutil
import re
from bs4 import BeautifulSoup
from seleniumbase import Driver
from sbvirtualdisplay import Display
from datetime import datetime
import os
import time          
import random
import duckdb
from dotenv import load_dotenv

print("Starting the hunt on TopCV with heavy weapons (SeleniumBase UC Mode)...")

# Load environment variables from the hidden .env file
load_dotenv()

# Connect to Database
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
conn = duckdb.connect(db_path)

# Retrieve cookie and headers (if configured in .env)
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

# ==========================================
# 2. CREATE CHROME DRIVER TO BYPASS CLOUDFLARE
# ==========================================

# Helper function to initialize a fresh Chrome instance
def get_new_driver():
    return Driver(
        uc=True,
        headless=False,       # Must be False for PyAutoGUI to work
        no_sandbox=True,
        browser="chrome"
    )

# Start Virtual Display (Xvfb)
print("Initializing Xvfb virtual display...")
display = Display(visible=0, size=(1920, 1080))
display.start()

# Initialize the first browser session
driver = get_new_driver()

# Crawl Settings
MAX_PAGES = 10
BATCH_SIZE = 5               # Number of pages to scrape before rotating the session
total_pages_scraped = 0
global_seen_job_ids = set()
jobs_data = []               # Master list to hold all extracted jobs

try:
    for keyword in keywords:
        print(f"\n========== Hunting for: {keyword.upper()} jobs ==========")
        page = 1
        max_retries = 10
        retry_count = 0

        while True:
            # ==========================================
            # SESSION ROTATION LOGIC (ANTI-BAN MECHANISM)
            # ==========================================
            if total_pages_scraped > 0 and total_pages_scraped % BATCH_SIZE == 0:
                print("\nRotating session: Closing current browser, resting, and launching a fresh instance...")
                try: driver.quit()
                except: pass
                
                # Cool down period to reset connection behaviors
                time.sleep(random.uniform(8.0, 12.0))
                driver = get_new_driver()
            # ==========================================

            print(f"Attacking {keyword.upper()} - Page {page}...")
            
            # Dynamically build the TopCV URL based on keyword
            url = f"https://www.topcv.vn/tim-viec-lam-{keyword}?page={page}"
            
            # ----------------------------------------------------
            # START REGION: BYPASS 403 CLOUDFLARE WITH SELENIUMBASE
            # ----------------------------------------------------
            try:
                # Use SeleniumBase to open the URL and bypass Cloudflare protection
                driver.uc_open_with_reconnect(url, reconnect_time=6)
                
                # Wait for DOM load and Cloudflare check to complete
                time.sleep(random.uniform(4.5, 6.5))

                # Force strictly 1920x1080 to match Xvfb (Crucial for PyAutoGUI coordinates)
                driver.set_window_size(1920, 1080)
                time.sleep(1)
                
                # Auto click into "I am human" if Cloudflare's screen pops up
                driver.uc_gui_click_captcha()
                time.sleep(2)
                
                # Get HTML source after the web has finished loading
                html_source = driver.page_source
                soup = BeautifulSoup(html_source, "html.parser")
                
                # Fallback check for Cloudflare blocks
                title_text = soup.title.text if soup.title else ""
                
                if "Access denied" in html_source or "Cloudflare" in title_text:
                    retry_count += 1
                    if retry_count >= max_retries:
                        print(f"[!] Max retries ({max_retries}) reached for {keyword.upper()} on page {page}. Cloudflare is too strict right now. Skipping to next keyword...")
                        break  # Escape the infinite loop
                        
                    print(f"Network lag or blocked (Cloudflare) at page {page}... (Attempt {retry_count}/{max_retries})")
                    print("Taking a 15-second break to fool the server and try again...")
                    time.sleep(15)
                    continue
                    
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"[!] Max retries ({max_retries}) reached due to timeout for {keyword.upper()} on page {page}. Skipping to next keyword...")
                    break  # Escape the infinite loop
                    
                print(f"Network lag or blocked (Timeout) at page {page}: {e} (Attempt {retry_count}/{max_retries})")
                print("Taking a 10-second break to fool the server and try again...")
                time.sleep(10)
                continue 
            # ----------------------------------------------------
            # END REGION: BYPASS 403 CLOUDFLARE
            # ----------------------------------------------------
            
            # Reset retry count because we successfully loaded the page
            retry_count = 0
            
            # Find Job Cards
            job_cards = soup.find_all("div", class_=lambda x: x and "job-item-search-result" in x.split() if x else False) 
            
            # THE "EMPTY PAGE" SKIP LOGIC
            if len(job_cards) == 0:
                print(f"[!] Page {page} is empty. No more jobs for {keyword.upper()}. Skipping to next keyword...")
                break
                
            print(f"Found {len(job_cards)} job cards on page {page}. Filtering duplicates...")
            
            # Count number of new jobs on this page
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
                        
                    # Cut off the query string (Tracking Parameters)
                    core_link = link.split('?')[0] 

                    id_match = re.search(r'/(\d+)\.html', core_link)
                    job_id = id_match.group(1) if id_match else core_link
                    
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
                    'job_title': job_title, 
                    'company_name': '', 
                    'location': '', 
                    'salary_raw': '',
                    'tech_stack': 'To be extracted by AI', 
                    'job_url': core_link, 
                    'source': 'TopCV',
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

            print(f"Captured {new_jobs_on_page} REAL jobs.")
            total_pages_scraped += 1

            if new_jobs_on_page == 0:
                print(f"[!] All jobs on page {page} are duplicates. Breaking the loop for {keyword.upper()}!")
                break 

            # ANTI-BAN SHIELD
            sleep_time = random.uniform(5.5, 9.5)
            print(f"Resting for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

            page += 1
            
            # Emergency brake in case the website bugs out and loops infinitely
            if page > MAX_PAGES: 
                print(f"Reached {MAX_PAGES} pages for {keyword.upper()}, applying emergency brake!")
                break

    # ==========================================
    # 3. SAVE DATA TO DUCKDB
    # ==========================================
    if jobs_data:
        print(f"\n[OK] Extraction complete! Preparing to insert {len(jobs_data)} new jobs into DuckDB...")
        for job in jobs_data:
            try:
                conn.execute("""
                    INSERT INTO raw_topcv_jobs (
                        job_id, job_url, job_title, company_name, location, 
                        salary_raw, tech_stack, source, crawl_timestamp, 
                        experience_level, job_category, job_description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (job_id) DO NOTHING
                """, (
                    job['job_id'], job['job_url'], job['job_title'], job['company_name'], job['location'],
                    job['salary_raw'], job['tech_stack'], job['source'], job['crawl_timestamp'],
                    job['experience_level'], job['job_category'], job['job_description']
                ))
            except Exception as e:
                print(f"Error inserting job {job['job_id']}: {e}")
        print("Data successfully loaded into the Bronze Layer (raw_topcv_jobs)!")
    else:
        print("\nNo new jobs to insert.")

finally:
    # Always clean up background processes
    print("\nClosing Chrome driver and Virtual Display...")
    try: driver.quit()
    except: pass
    try: display.stop()
    except: pass
    conn.close()
    print("Pipeline finished successfully.")