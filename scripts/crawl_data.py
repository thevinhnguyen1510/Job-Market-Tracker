from curl_cffi import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time          
import random
import duckdb

print("Starting the hunt on ITviec with heavy weapons...")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Referer': 'https://itviec.com/',
    # Keeping your provided cookie for authentication
    'Cookie': '_ga=GA1.1.1611562945.1769744266; _fbp=fb.1.1769744266466.724016515880833962; viewed_jobs=yyDJminPpjGFabXHVsL9ZNiK0ybkHr67s4AZ4s5lOnLVFZZcmnbc2wHIRmH5tr30eTXrU5%2BhT4wIp7ZambEc87CUeANGbJ3TEji%2BeITVTtJaE8g6c0YCu%2Fa2R1n8pQ1nmxO%2F8RWcZIUlMnz3CxmFpgGe2O4uoR3bsubCWqqP1fFS8XDb7aTkXWJqBUuhnpMe8F0hBYdnLK7O7LtVawFRJqmDMZf3hIbVTLbAltT5uT%2FXDF8AwI0wICPBoo8uuvUHg4etva5Qd5qll6zbib0yi9ebJ0CiuWUyXPLUycxFwh0fNB8631gOj6BVjxChecK6kRtMIV9LgHvWiTf4rpGPIQCuP38ASDzt1VFFNocuOZPLM1UfNNnQRXCLZkGzhtkqZmy0RLSZZ9L0HuExE4tnS3MaauS%2BAVTZ5ib5zAASmgS4AlxgzB0Ry8PKvZbMDs06tc5JbF1u%2B%2FFDlcqtaVfQC5zo56lGBiL4pHWVwzUmupMiQ4Ei%2ByqMLZxJSjWbnZqB0G96Ni6TWrcme4Bl9BQAHEhPnVDWclxhpVIYEso3PSx1--ls4EfyBQV%2FqdQ7h0--JMRd6yHmlJdA3Lkkv%2Fj9tQ%3D%3D; _gcl_au=1.1.1210157395.1769744265.95573194.1772855774.1772855801; _fbc=fb.1.1773929215393.IwY2xjawQo4lFleHRuA2FlbQIxMQBzcnRjBmFwcF9pZBAyMjIwMzkxNzg4MjAwODkyAAEeb_GGBGGpe8vCBIRJfNyE6qCADeb4Y8Iia0hIxV71lCmPsgACEYIPwiDXFFM_aem_2RGjDRKp5ZrUmOwjQiA0ww; device_id=eyJfcmFpbHMiOnsibWVzc2FnZSI6IklqWTVPREF4TVRKbUxUazBaamd0TkdFNE5DMWhZakEyTFRjd05ESTFabU5sTmpJM1pTST0iLCJleHAiOm51bGwsInB1ciI6ImNvb2tpZS5kZXZpY2VfaWQifX0%3D--94065392ddde2ccd722b4adfbc2952c8d4c02d79; recent_searches=8G6PzWuG5mIuIzLx2TmUYWziU6v5ZbeWEOEJ6VHQwoeEoMmiqeMm5gLwX4Uc5%2F%2Bpbj2z8AFNdqhG8P1Y5rKtT52Y7dswpOd5ZRi6K3ZZ0TYHDsFhnULx--9gFaLpMuh8fhT4RB--68FmYnBcVefKnxP5x35FmA%3D%3D; search_query=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltUmhkR0V0Wlc1bmFXNWxaWElpIiwiZXhwIjpudWxsLCJwdXIiOiJjb29raWUuc2VhcmNoX3F1ZXJ5In19--1a2bd420b6c6bdeb42498cc0265aceca0a0fc229; count_promote_review_popup=1; g_state={"i_l":1,"i_ll":1774172446198,"i_b":"WXFpz+NOZyb8YvbMHJvEFR9i19Yf30I0zJNrdsPlYds","i_e":{"enable_itp_optimization":3},"i_p":1774179591465}; cf_clearance=xiiXwtL0hz6I7kYTrRfOmTXHx_b6Ie93CpFlZQDJOJI-1774172454-1.2.1.1-mS436UdzZS62MEfhHuGdYBB1IWBaVaWwvzdOu11HWX_uAMyGxX6DjFva0DV1P_wUDkYj7Txuz2dtYjJO0c3SZdF5OzMJ6FezKi5Ou2nMdNLjfxPclxRWu3VrB0Oka9h3yHeBFqxNPboFXLbiQZbAFeI_RsxmShwYhE15w15sSfbc.wV6yOgCEVeLjr9CVwWh29NvulQ9EooBP4xz76JmxONXpz3FP4IA8m1s2eDy_r8; auth_token=jqomdxvpm2aOI%2BZ1Kp8R6Yj3vg2ZVR5qA0js6K45uUryjzd%2FunQgnfosiPLVoFApcPph2WYOVA9yGkcZqfOGXQKa592blkLcLvEJ19lzlFw9p7Ku0Ijp%2F0C0FDNMOkJtdzeBSKoAH5IC6f1Sf%2Bj6pku7MmCIZQEdl8Pcn%2BQ3fh4XClqKVXBYva6uLtuLyZl2ALtJaYo7WroRFSlZi1koPeZeQPFeLGnKU9Im3SHi98ej9BDuRCYXHr6dh4BC%2Fye4kTmbihMv%2FyAheVpdXkNdKjhRRFabIVpc%2Fkfe7QfSFul2jS2qdVE%2F4y66JH6XFshr0gC6gVJ8oPiyjKjQM89G1um0yMQ%3D--LNygzAZkT09sKb6w--CaHOLzzqgYcniLszEpwoLQ%3D%3D; _ga_H9FSZTPEGR=GS2.1.s1774172445$o11$g1$t1774172451$j54$l1$h1428972635; _ITViec_session=BSpn9CS9VkiTuctTeS7osFaCGclopptmmT0CZj2l6%2F%2FhNSIUDO7jFxvA139P4Ve6xD9Z%2B7f0ZmOUpIarwuo7Y6eLgL49zXcgQ2GnWS04bBIQHwmLJZtmIE6rtMxweNjlmKHYw88SJXoeIETLwc8CrOYZPpLY7rwSOFMfcbLQ1SyMpuTYEdzmtvg5lYNk1wAiG41B4NuBYjwdNCQfKucD8Qjg9NNKKE41A4TZVetepaDHcei4X2j0CAPUEIaixwa0Nu9Bib40DCeB1tfvxBhUFMfB9RY64MPD1dXGbitcvDfRY%2Bq%2BBnJ203%2BGH9VE9Y1iYzS8iJpJpB0JF5M0c%2BSX8VHQ3b8D1xxhw9t4JnLW2%2B46AVicbElmGU5ECaSTJYtV8LksQ0vGwb86fcaeqxmk5YgvOpCmjIJyn4WoDceNKQWniJHnyHQRab6DHxgJ3MjsVJiqyBLXTiPTa%2BQcW7qQOA47d2rQm%2FmZLFc842NgicxGmofd5SGGeaHjPYEwRd1yV34J0OZDumHj0wbqTqCpbRFJsDGXJcQcgaLczPWhmkETnSkeFBNJTckOWPK7N5p%2BTGRIA5E5%2F0Oxctqg5Ydp%2FbLP1hcdvv6vpxsmbXDClRAFnJ5g%2B7Tu7iv7SU7kkbJIdAhrtna0r1FX4q3EIpgwetUUHQCgtyiIihT3f%2FcoRaKe1gHJEV7jQW2OIzv91fuRGKk%2FyBnNqjSMFA1c6Wicrz8oaZCkvIV84pmgVlm7M8MhfEZANGZKlQJ3dztPSCjOYfLk4jluoJAgbidlYMqa7XjYMjj1nGChuEGVc6xxTtSEpbMLxUeBHGjbYsoThhR2bXw%2Bu1jW10g3%2FMZTgxEargIIpWIcMXg%2F9Uu182QKu5fVgESrl52kLqfFpTxXU6XFzqgu2%2BpC6y5I3j2iVirCqsM4T2Jvog8kG9BwZA7x%2BqhPG6N9jS5Q1rXGGS3U%2B2E59IHCVuZLcg9GnkkTs%2FhAmRnXPM%2FXd2pplqpIQCgyJ%2BHyzYqR7jC%2BumailO%2BoiQ1iGN8Wctp0xzGWd7g6BA%3D%3D--JRs2FH2CwVe%2BXzDx--dB5Vtw8bO1qvhk%2FwMfap6Q%3D%3D' 
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

for keyword in keywords:
    print(f"\n========== Hunting for: {keyword.upper()} jobs ==========")
    page = 1

    while True:
        print(f"---> Attacking {keyword.upper()} - Page {page}...")
        
        # FIX 1: Make URL dynamic based on the current keyword in the loop
        url = f"https://itviec.com/it-jobs/{keyword}?page={page}"
        
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
            job_cards = soup.find_all("div", class_="job-card") 
            
            # THE "EMPTY PAGE" SKIP LOGIC: Fail Fast!
            # If 0 jobs are found, we've reached the end of this keyword. Break the while loop.
            if len(job_cards) == 0:
                print(f"[!] Page {page} is empty. No more jobs for {keyword.upper()}. Skipping to next keyword...")
                break 
                
            print(f"Captured {len(job_cards)} jobs.")
            
            # Extract data from each job card
            for card in job_cards:
                job_info = {
                    'job_title': '', 'company_name': '', 'location': '', 'salary_raw': '',
                    'tech_stack': '', 'job_url': '', 'source': 'ITViec', 
                    'crawl_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'experience_level': '', 
                    'job_category': keyword, # FIX 2: Dynamically map the category to the keyword
                    'job_description': '' 
                }
                
                # --- Get Job Title & Link URL ---
                try:
                    title_element = card.find("h3") 
                    job_info['job_title'] = title_element.text.strip()
                    job_info['job_url'] = title_element.get('data-url', '') 
                except:
                    job_info['job_title'] = "Title extraction error"

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

        else:
            print(f"Blocked at page {page}! Error code: {response.status_code}")
            break # Break out of the inner while loop to try the next keyword
        
        # ANTI-BAN SHIELD
        sleep_time = random.uniform(3.5, 6.2)
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
    db_path = '../job_market.duckdb' # Make sure the path is correct when running with Docker/Airflow
    conn = duckdb.connect(db_path)
    
    # 3.1. Ensure table raw_itviec_jobs exists (with PRIMARY KEY to prevent duplicates)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_itviec_jobs (
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
                INSERT INTO raw_itviec_jobs (
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
            
            # If insertion is successful (no duplicate) and cursor changes number of rows
            # Note: DuckDB returns information via fetchone() for INSERT commands
            # But for simplicity, we just count the total number of attempts
            new_jobs_count += 1 
            
        except Exception as e:
            print(f"Error inserting job {job['job_title']}: {e}")
            
    # Statistics
    total_raw = conn.execute("SELECT COUNT(*) FROM raw_itviec_jobs").fetchone()[0]
    conn.close()
    
    print(f" MISSION ACCOMPLISHED!")
    print(f"   - Number of jobs collected this time: {len(jobs_data)}")
    print(f"   - Total number of jobs in Database (Bronze Layer): {total_raw}")
else:
    print("\nMission failed: No data collected.")