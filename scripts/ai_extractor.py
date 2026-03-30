import duckdb
import json
import time
import os
from dotenv import load_dotenv
from openai import OpenAI

# 1. SETUP OPENAI API
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("ERROR: Not found OPENAI_API_KEY in .env file!")

client = OpenAI(api_key=OPENAI_API_KEY)

# 2. CONNECT TO DUCKDB & SETUP ARCHITECTURE
db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

# Create Silver layer table
conn.execute("""
    CREATE TABLE IF NOT EXISTS silver_itviec_jobs (
        job_url VARCHAR PRIMARY KEY,
        job_title VARCHAR,
        min_years_of_experience INTEGER,
        ai_core_tech_stack VARCHAR,
        english_requirement VARCHAR,
        ai_job_role VARCHAR,
        job_level VARCHAR,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")

# 3. IDENTIFY PENDING JOBS (Left Join to find jobs in Raw but not in Silver)
pending_jobs = conn.execute("""
    SELECT DISTINCT r.job_url, r.job_title, r.job_description 
    FROM raw_itviec_jobs r
    LEFT JOIN silver_itviec_jobs s ON r.job_url = s.job_url
    WHERE s.job_url IS NULL AND r.job_description IS NOT NULL
""").fetchall()

total_pending = len(pending_jobs)
print(f"Total jobs to process: {total_pending}\n")

if total_pending == 0:
    print("All data has been pushed to Silver layer. No need to run AI.")
    conn.close()
    exit()

# 4. THE EXTRACTION LOOP
for index, job in enumerate(pending_jobs):
    job_url, job_title, job_desc = job
    print(f"[{index + 1}/{total_pending}] Processing: {job_title[:50]}...")
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": "You are an elite Data Engineer and HR Tech Analyst. Your task is to extract structured data from IT Job Descriptions into a STRICT JSON format. Do not hallucinate. Never output markdown outside the JSON."
                },
                {
                    "role": "user", 
                    "content": f"""
                    Analyze this Job Description and extract exactly 5 fields according to these STRICT RULES:

                    1. "min_years_of_experience" (integer):
                       - Extract MINIMUM years required. If range (e.g., "1-3"), pick lower bound (1).
                       - If Fresher, Intern, or not mentioned, output 0.

                    2. "core_tech_stack" (list of strings):
                       - Max 5 HARD technical skills (languages, frameworks, DBs, clouds).
                       - NO generic tools (Jira, Git) or soft skills. Standardize names.

                    3. "english_requirement" (string):
                       - Map to ONE of: "Basic", "Intermediate", "Fluent", or "Not mentioned".

                    4. "job_role" (string):
                       - Map to ONE of: "Backend", "Frontend", "Fullstack", "Mobile", "Data Engineer", "Data Scientist", "Data/Business Analyst", "Product Owner/Manager", "QA/QC/Tester", "DevOps/Cloud", "System Admin", "UI/UX Designer", "Security", "Scrum Master", "AI/Machine Learning".
                       - STRICT RULE: DO NOT include levels (Senior, Junior) in this field.

                    5. "job_level" (string):
                       - Map to ONE of: "Intern", "Fresher", "Junior", "Middle", "Senior", "Manager", "Director".
                       - IF NOT STATED, infer from min_years_of_experience: 0=Fresher, 1-2=Junior, 3-4=Middle, 5+=Senior. Default: "Unknown".

                    ### JOB DESCRIPTION START ###
                    {job_desc}
                    ### JOB DESCRIPTION END ###
                    """
                }
            ],
            response_format={ "type": "json_object" }
        )
        
        extracted_data = json.loads(response.choices[0].message.content)
        
        # Parse data
        years = extracted_data.get("min_years_of_experience", 0)
        tech_stack = ", ".join(extracted_data.get("core_tech_stack", []))
        english = extracted_data.get("english_requirement", "Not mentioned")
        role = extracted_data.get("job_role", "Unknown")
        level = extracted_data.get("job_level", "Unknown")
        
        # INSERT directly into Silver layer
        conn.execute("""
            INSERT INTO silver_itviec_jobs (
                job_url, job_title, min_years_of_experience, ai_core_tech_stack, 
                english_requirement, ai_job_role, job_level
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (job_url) DO NOTHING
        """, (job_url, job_title, years, tech_stack, english, role, level))
        
        print(f"Success: [{level}] {role}")
        
    except Exception as e:
        print(f"Failed: {e}")
        # Mark error to avoid reprocessing
        conn.execute("""
            INSERT INTO silver_itviec_jobs (job_url, job_title, job_level)
            VALUES (?, ?, 'Error')
            ON CONFLICT (job_url) DO NOTHING
        """, (job_url, job_title))

    time.sleep(0.5)

print("\nALL DATA TRANSFERRED TO SILVER LAYER SUCCESSFULLY!")
conn.close()