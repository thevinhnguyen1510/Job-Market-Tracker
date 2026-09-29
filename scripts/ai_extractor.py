import os
import sys 
import json
import asyncio
import duckdb
import instructor
from openai import AsyncOpenAI
from pydantic import BaseModel, Field, model_validator
from typing import List, Literal, Tuple
from dotenv import load_dotenv
from enum import Enum
from datetime import datetime

# ==========================================
# 0. DYNAMIC SOURCE CONFIGURATION
# ==========================================
TARGET_SOURCE = sys.argv[1].lower() if len(sys.argv) > 1 else 'itviec'

if TARGET_SOURCE not in ['itviec', 'topcv']:
    print(f"ERROR: Invalid source '{TARGET_SOURCE}'. Please use 'itviec' or 'topcv'.")
    sys.exit(1)

RAW_TABLE = f"raw_{TARGET_SOURCE}_jobs"
AI_EXTRACTIONS_TABLE = "raw_ai_extractions"

print("=" * 70)
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INITIATING ASYNC AI EXTRACTOR FOR [{TARGET_SOURCE.upper()}]...")
print("=" * 70)

# ==========================================
# 1. SETUP ASYNC OPENAI WITH INSTRUCTOR
# ==========================================
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("ERROR: OPENAI_API_KEY not found in .env file!")

# Initialize Async client with backward/forward compatibility for instructor versions
raw_async_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

try:
    client = instructor.from_openai(raw_async_client)
except AttributeError:
    try:
        client = instructor.patch(raw_async_client)
    except Exception:
        client = getattr(instructor, "apatch", instructor.patch)(raw_async_client)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

# ==========================================
# 2. CREATE AI EXTRACTIONS TABLE & FETCH PENDING JOBS
# ==========================================
conn = duckdb.connect(db_path)
try:
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {AI_EXTRACTIONS_TABLE} (
            job_id VARCHAR PRIMARY KEY,  
            min_years_of_experience INTEGER,
            ai_core_tech_stack VARCHAR, 
            english_requirement VARCHAR,
            ai_job_role VARCHAR,
            job_level VARCHAR,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Retrieve jobs needing AI extraction (skip EXPIRED, connection errors, or empty)
    pending_jobs = conn.execute(f"""
        SELECT DISTINCT r.job_id, r.job_url, r.job_title, r.job_description 
        FROM {RAW_TABLE} r
        LEFT JOIN {AI_EXTRACTIONS_TABLE} a ON r.job_id = a.job_id
        WHERE a.job_id IS NULL 
          AND r.job_description IS NOT NULL
          AND r.job_description != ''
          AND r.job_description != 'JD content not found'
          AND r.job_description != 'Connection error'
          AND r.job_description != 'EXPIRED'
    """).fetchall()

finally:
    # CLOSE DB CONNECTION IMMEDIATELY TO RELEASE DUCKDB FILE LOCK!
    conn.close()

total_pending = len(pending_jobs)
print(f"-> Total pending jobs to process for {TARGET_SOURCE.upper()}: {total_pending}\n")

if total_pending == 0:
    print(f"All data for {TARGET_SOURCE.upper()} is already processed. No work needed.")
    sys.exit(0)

# ==========================================
# 3. PYDANTIC DATA CONTRACT SCHEMA
# ==========================================
class EnglishLevel(str, Enum):
    NONE = "Not mentioned"
    READING = "Read/Write basic documentations"
    COMMUNICATION = "Communicate with clients"
    CERTIFICATE = "Required certificate (IELTS/TOEIC)"

class JobExtraction(BaseModel):
    min_years_of_experience: int = Field(
        ..., 
        description="Extract MINIMUM practical years. '1-3 years' -> 1. 'Fresher/Intern/None' -> 0."
    )
    core_tech_stack: List[str] = Field(
        ..., 
        max_length=5, 
        description="Max 5 hard tech skills only. Ex: ['Python', 'SQL', 'AWS', 'PostgreSQL']."
    )
    english_requirement: EnglishLevel = Field(
        ..., 
        description="Classify English requirement from JD."
    )
    job_role: Literal[
        "Backend", "Frontend", "Fullstack", "Mobile", "Data Engineer", 
        "Data Scientist", "Data/Business Analyst", "Product Owner/Manager", 
        "QA/QC/Tester", "DevOps/Cloud", "System Admin", "UI/UX Designer", 
        "Security", "Scrum Master", "AI/Machine Learning", "Unknown"
    ] = Field(..., description="Standard job role. General Software Engineer/Developer roles map to 'Backend' unless frontend/mobile is specified.")
    
    job_level: Literal["Intern", "Fresher", "Junior", "Middle", "Senior", "Manager", "Director", "Unknown"] = Field(
        ..., description="Standard seniority level."
    )

    @model_validator(mode='after')
    def infer_job_level(self) -> 'JobExtraction':
        if self.job_level == "Unknown":
            years = self.min_years_of_experience
            if years == 0: self.job_level = "Fresher"
            elif 1 <= years <= 2: self.job_level = "Junior"
            elif 3 <= years <= 4: self.job_level = "Middle"
            elif years >= 5: self.job_level = "Senior"
        return self

# ==========================================
# 4. ASYNC EXTRACTION WORKER WITH SEMAPHORE
# ==========================================
CONCURRENCY_LIMIT = 6
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

async def extract_single_job(index: int, total: int, job: Tuple) -> Tuple:
    job_id, job_url, job_title, job_desc = job
    
    # Trim JD to 2200 characters to optimize token consumption
    trimmed_desc = job_desc[:2200]
    
    async with semaphore:
        for attempt in range(2):
            try:
                extracted: JobExtraction = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    response_model=JobExtraction,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are an elite Data Engineer and HR Tech Analyst. Extract structured data from IT Job Descriptions. "
                                "IMPORTANT: In the tech market, 'Software Engineer' and 'Software Developer' roles focusing on "
                                "server-side, microservices, databases, or languages (Java, C#, .NET, Python, Golang, C++) must be classified as 'Backend'."
                            )
                        },
                        {
                            "role": "user",
                            "content": f"Title: {job_title}\n\n### JD ###\n{trimmed_desc}"
                        }
                    ],
                    max_retries=1
                )
                print(f"[{index + 1}/{total}] [OK] Extracted: [{extracted.job_level}] {extracted.job_role} - {job_title[:35]}")
                return (
                    job_id,
                    extracted.min_years_of_experience,
                    json.dumps(extracted.core_tech_stack),
                    extracted.english_requirement.value,
                    extracted.job_role,
                    extracted.job_level
                )
            except Exception as e:
                if attempt == 1:
                    print(f"[{index + 1}/{total}] [FAIL] Error extracting {job_id}: {e}")
                    return (
                        job_id, 0, "[]",
                        EnglishLevel.NONE.value, "Unknown", "Error"
                    )
                await asyncio.sleep(1.0)

async def main():
    tasks = [
        extract_single_job(idx, total_pending, job) 
        for idx, job in enumerate(pending_jobs)
    ]
    
    results = await asyncio.gather(*tasks)
    
    # ==========================================
    # 5. BULK INSERT INTO DUCKDB
    # ==========================================
    print(f"\nBulk inserting {len(results)} extracted records into {AI_EXTRACTIONS_TABLE}...")
    db_conn = duckdb.connect(db_path)
    try:
        db_conn.executemany(f"""
            INSERT INTO {AI_EXTRACTIONS_TABLE} (
                job_id, min_years_of_experience,
                ai_core_tech_stack, english_requirement, ai_job_role,
                job_level, processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (job_id) DO UPDATE SET 
                min_years_of_experience = EXCLUDED.min_years_of_experience,
                ai_core_tech_stack = EXCLUDED.ai_core_tech_stack,
                english_requirement = EXCLUDED.english_requirement,
                ai_job_role = EXCLUDED.ai_job_role,
                job_level = EXCLUDED.job_level,
                processed_at = EXCLUDED.processed_at;
        """, results)
        
        success_total = sum(1 for r in results if r[5] != 'Error')
        print(f"-> [SUCCESS] Saved {success_total}/{total_pending} jobs successfully into {AI_EXTRACTIONS_TABLE}!")
    finally:
        db_conn.close()

if __name__ == "__main__":
    asyncio.run(main())