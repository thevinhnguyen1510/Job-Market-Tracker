import duckdb
import json
import time
from openai import OpenAI
from dotenv import load_dotenv
import os

print("🚀 ACTIVATING PAID PIPELINE: OPENAI ENRICHMENT...")

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# 1. SETUP OPENAI API
client = OpenAI(api_key=OPENAI_API_KEY)

# 2. CONNECT TO DUCKDB
db_path = '../job_market.duckdb'
conn = duckdb.connect(db_path)

conn.execute("ALTER TABLE raw_itviec_jobs ADD COLUMN IF NOT EXISTS min_years_of_experience INTEGER;")
conn.execute("ALTER TABLE raw_itviec_jobs ADD COLUMN IF NOT EXISTS ai_core_tech_stack VARCHAR;")
conn.execute("ALTER TABLE raw_itviec_jobs ADD COLUMN IF NOT EXISTS english_requirement VARCHAR;")
conn.execute("ALTER TABLE raw_itviec_jobs ADD COLUMN IF NOT EXISTS job_level VARCHAR;")
conn.execute("ALTER TABLE raw_itviec_jobs ADD COLUMN IF NOT EXISTS ai_job_role VARCHAR;")

# Get pending jobs (job_level is NULL or 'Error')
pending_jobs = conn.execute("""
    SELECT job_url, job_title, job_description 
    FROM raw_itviec_jobs 
    WHERE job_description IS NOT NULL 
      AND (job_level IS NULL OR job_level = 'Error')
""").fetchall()

total_pending = len(pending_jobs)
print(f"Total jobs to process: {total_pending}\n")

# 3. THE EXTRACTION LOOP
for index, job in enumerate(pending_jobs):
    job_url, job_title, job_desc = job
    
    print(f"[{index + 1}/{total_pending}] Processing: {job_title[:50]}...")
    
    try:
        # Call model gpt-4o-mini with 'json_object' mode to ensure no format errors
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": "You are an elite Data Engineer and HR Tech Analyst. Your task is to extract structured data from IT Job Descriptions (which may be in English or Vietnamese) into a STRICT JSON format. Do not hallucinate. Never output markdown outside the JSON."
                },
                {
                    "role": "user", 
                    "content": f"""
                    Analyze this Job Description and extract exactly 5 fields according to these STRICT RULES:

                    1. "min_years_of_experience" (integer):
                       - Extract the MINIMUM years of experience required.
                       - If a range is given (e.g., "1 to 3 years"), extract the lower bound (1).
                       - If stated as "Fresher", "Intern", "No experience needed", or not mentioned, output 0.

                    2. "core_tech_stack" (list of strings):
                       - Extract MAXIMUM 5 HARD technical skills (programming languages, frameworks, databases, cloud platforms).
                       - DO NOT include generic tools (Jira, Slack, Git) or soft skills (Teamwork, Agile).
                       - Standardize names if possible (e.g., "NodeJS" -> "Node.js", "React" -> "ReactJS").
                       - If none found, output [].

                    3. "english_requirement" (string):
                       - Map to EXACTLY ONE of these standard levels:
                         * "Basic" (Reading docs / Đọc hiểu tài liệu)
                         * "Intermediate" (Communication / Giao tiếp cơ bản)
                         * "Fluent" (Working directly with foreigners / Thành thạo)
                         * "Not mentioned" (If absolutely no context is found).

                    4. "job_role" (string):
                       - Map the CORE role to EXACTLY ONE of these categories: "Backend", "Frontend", "Fullstack", "Mobile", "Data Engineer", "Data Scientist", "Data/Business Analyst", "Product Owner/Manager", "QA/QC/Tester", "DevOps/Cloud", "System Admin", "UI/UX Designer", "Security", "Scrum Master", "AI/Machine Learning".
                       - STRICT RULE: DO NOT include levels (Senior, Junior, Lead) in this field.
                       - If hybrid (e.g., Backend & DevOps), pick the PRIMARY focus. If unmapped, extract the shortest exact role name in English.

                    5. "job_level" (string):
                       - Map to EXACTLY ONE of: "Intern", "Fresher", "Junior", "Middle", "Senior", "Manager", "Director".
                       - If explicitly stated in the title or JD, use it.
                       - IF NOT STATED, you MUST infer based on "min_years_of_experience" using this strict heuristic:
                         * 0 years = "Fresher"
                         * 1 to 2 years = "Junior"
                         * 3 to 4 years = "Middle"
                         * 5+ years = "Senior"
                       - Output "Unknown" ONLY if completely impossible to infer.

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
        
        # Update Database
        conn.execute("""
            UPDATE raw_itviec_jobs 
            SET min_years_of_experience = ?, ai_core_tech_stack = ?, english_requirement = ?, ai_job_role = ?, job_level = ?
            WHERE job_url = ?
        """, (years, tech_stack, english, role, level, job_url))
        
        print(f"Success: {level} | {tech_stack[:30]}...")
        
    except Exception as e:
        print(f"Failed: {e}")
        conn.execute("UPDATE raw_itviec_jobs SET job_level = 'Error' WHERE job_url = ?", (job_url,))

    time.sleep(0.5)

print("\nALL DATA ENRICHED SUCCESSFULLY!")
conn.close()