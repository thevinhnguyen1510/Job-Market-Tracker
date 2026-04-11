import os
import sys # ADDED: To read command-line arguments for dynamic source
import time
import json
import duckdb
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field, model_validator
from typing import List, Literal
from dotenv import load_dotenv
from enum import Enum

# ==========================================
# 0. DYNAMIC SOURCE CONFIGURATION
# ==========================================
# Accept source from terminal (e.g., python ai_extractor.py topcv). Default to 'itviec'.
TARGET_SOURCE = sys.argv[1].lower() if len(sys.argv) > 1 else 'itviec'

if TARGET_SOURCE not in ['itviec', 'topcv']:
    print(f"ERROR: Invalid source '{TARGET_SOURCE}'. Please use 'itviec' or 'topcv'.")
    sys.exit(1)

# Dynamically set table names based on the target source
RAW_TABLE = f"raw_{TARGET_SOURCE}_jobs"
SILVER_TABLE = f"silver_all_jobs"

print(f"INITIATING SPRINT 1: AI MASS EXTRACTOR FOR [{TARGET_SOURCE.upper()}] (SILVER LAYER)...")

# 1. SETUP OPENAI API WITH INSTRUCTOR
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("ERROR: OPENAI_API_KEY not found in .env file!")

# Wrap the standard OpenAI client with Instructor to enforce Pydantic schemas
client = instructor.from_openai(OpenAI(api_key=OPENAI_API_KEY))

# ==========================================
# 2. CONNECT TO DUCKDB & SETUP ARCHITECTURE
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
conn = duckdb.connect(db_path)

# Create Silver layer table dynamically 
conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        job_url VARCHAR PRIMARY KEY,
        job_title VARCHAR,
        min_years_of_experience INTEGER,
        ai_core_tech_stack VARCHAR, 
        english_requirement VARCHAR,
        ai_job_role VARCHAR,
        job_level VARCHAR,
        source VARCHAR,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR DEFAULT 'Active'
    );
""")

# 3. DEFINE THE STRICT DATA CONTRACT (PYDANTIC SCHEMA)
class EnglishLevel(str, Enum):
    NONE = "Not mentioned"
    READING = "Read/Write basic documentations"
    COMMUNICATION = "Communicate with clients"
    CERTIFICATE = "Required certificate (IELTS/TOEIC)"

class JobExtraction(BaseModel):
    min_years_of_experience: int = Field(
        ..., 
        description="Extract MINIMUM years. Rules: '1-3 years' -> 1. '6 months' -> 0. Fresher/Intern/Not mentioned -> 0."
    )
    
    core_tech_stack: List[str] = Field(
        ..., 
        max_length=5, 
        description="Max 5 HARD technical skills. Standardize names. Example: ['React', 'Node.js', 'AWS', 'PostgreSQL']." # TỐI ƯU 2: Thêm example
    )
    
    english_requirement: EnglishLevel = Field(
        ..., 
        description="""Read the Job Description and classify the English requirement:
        - Choose 'Not mentioned' if the JD does not mention English at all.
        - Choose 'Read/Write basic documentations' if it only requires reading basic English documents.
        - Choose 'Communicate with clients' if it requires working with foreign teams, writing emails, or chatting.
        - Choose 'Required certificate (IELTS/TOEIC)' if it clearly states a score or requires excellent English.
        """
    )
    
    job_role: Literal["Backend", "Frontend", "Fullstack", "Mobile", "Data Engineer", "Data Scientist", "Data/Business Analyst", "Product Owner/Manager", "QA/QC/Tester", "DevOps/Cloud", "System Admin", "UI/UX Designer", "Security", "Scrum Master", "AI/Machine Learning", "Unknown"] = Field(
        ..., 
        description="Main job role. Do not include seniority."
    )
    
    job_level: Literal["Intern", "Fresher", "Junior", "Middle", "Senior", "Manager", "Director", "Unknown"] = Field(
        ..., 
        description="Extract exact job level from title or JD. If absolutely NOT stated anywhere, output 'Unknown'."
    )

    @model_validator(mode='after')
    def infer_job_level(self) -> 'JobExtraction':
        if self.job_level == "Unknown":
            years = self.min_years_of_experience
            if years == 0:
                self.job_level = "Fresher"
            elif 1 <= years <= 2:
                self.job_level = "Junior"
            elif 3 <= years <= 4:
                self.job_level = "Middle"
            elif years >= 5:
                self.job_level = "Senior"
        return self

# 3.5. BULK UPDATE LAST SEEN (PREVENT EXPIRED JOBS)
    print(f"Updating Last Seen for existing jobs...")
    conn.execute(f"""
        UPDATE {SILVER_TABLE}
        SET last_seen_at = CURRENT_TIMESTAMP, 
            status = 'Active'
        WHERE job_url IN (SELECT job_url FROM {RAW_TABLE})
    """)
    print("Updated Last Seen for existing jobs. Moving to find new jobs...")

# 4. IDENTIFY PENDING JOBS (Left Join to find jobs in dynamically selected Raw but not in Silver)
try:
    pending_jobs = conn.execute(f"""
        SELECT DISTINCT r.job_url, r.job_title, r.job_description 
        FROM {RAW_TABLE} r
        LEFT JOIN {SILVER_TABLE} s ON r.job_url = s.job_url
        WHERE s.job_url IS NULL 
          AND r.job_description IS NOT NULL
          AND r.job_description != 'JD content not found'
          AND r.job_description != 'Connection error'
    """).fetchall()
except duckdb.CatalogException:
    print(f"ERROR: Table {RAW_TABLE} does not exist. Please run the crawl script first.")
    conn.close()
    exit()

total_pending = len(pending_jobs)
print(f" Total jobs to process for {TARGET_SOURCE.upper()}: {total_pending}\n")

if total_pending == 0:
    print(f"All data for {TARGET_SOURCE.upper()} has been successfully pushed to the Silver layer. No pending jobs found.")
    conn.close()
    exit()

# 5. THE EXTRACTION LOOP (WITH AUTO-RETRY)
success_count = 0

for index, job in enumerate(pending_jobs):
    job_url, job_title, job_desc = job
    print(f"[{index + 1}/{total_pending}] Processing: {job_title[:50]}...")
    
    try:
        # Instructor handles the function calling, parsing, and retries automatically
        extracted_data = client.chat.completions.create(
            model="gpt-4o-mini",
            response_model=JobExtraction, # Enforce the Pydantic schema
            messages=[
                {
                    "role": "system", 
                    "content": "You are an elite Data Engineer and HR Tech Analyst. Your task is to extract structured data from IT Job Descriptions."
                },
                {
                    "role": "user", 
                    "content": f"Title: {job_title}\n\n### JOB DESCRIPTION ###\n{job_desc[:3500]}" # Truncate to save tokens if JD is abnormally long
                }
            ],
            max_retries=2 # Magic happens here: If the LLM violates the schema, Instructor catches the error and forces it to fix itself.
        )
        
        # INSERT directly into Silver layer using dot notation and f-strings for dynamic tables
        conn.execute(f"""
            INSERT INTO {SILVER_TABLE} (
                job_url, job_title, min_years_of_experience, ai_core_tech_stack, 
                english_requirement, ai_job_role, job_level, source, last_seen_at, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'Active')
            ON CONFLICT (job_url) DO UPDATE SET 
                last_seen_at = EXCLUDED.last_seen_at,
                status = 'Active'
        """, (
            job_url, job_title, 
            extracted_data.min_years_of_experience, 
            json.dumps(extracted_data.core_tech_stack), 
            extracted_data.english_requirement.value, 
            extracted_data.job_role, 
            extracted_data.job_level,
            TARGET_SOURCE.upper()
        ))
        
        success_count += 1
        print(f"-> Success: [{extracted_data.job_level}] {extracted_data.job_role}")
        
    except Exception as e:
        print(f"Failed: {e}")
        # Mark error in the DB to avoid infinite reprocessing loops
        conn.execute(f"""
            INSERT INTO {SILVER_TABLE} (job_url, job_title, job_level, source, status)
            VALUES (?, ?, 'Error', ?, 'Error')
            ON CONFLICT (job_url) DO NOTHING
        """, (job_url, job_title, TARGET_SOURCE.upper()))

    # Anti-rate-limit shield
    time.sleep(0.5)

print(f"\nMISSION ACCOMPLISHED! Successfully transferred {success_count}/{total_pending} jobs to {SILVER_TABLE}.")
conn.close()