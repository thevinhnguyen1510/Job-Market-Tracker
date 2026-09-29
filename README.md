# 🚀 IT Job Market Tracker & AI Career Coach

> **An Enterprise-Grade End-to-End Modern Data & AI Platform**
> Daily automated collection, intelligent cleaning, AI enrichment, vector search, and interactive career advisory for the Vietnamese IT job market.

---

## 🌟 What Is This Project? (In Simple Words)

Imagine having a tireless personal assistant who wakes up every morning at 7:00 AM, browses Vietnam's biggest tech hiring platforms (**ITviec** and **TopCV**), and does the following:

1. **Finds Every New Tech Job:** Scans hundreds of job postings across Software Engineering, Data, DevOps, AI, and Product.
2. **Filters Out the Noise:** Detects and flags expired postings immediately so you never waste time applying to closed positions.
3. **Reads Between the Lines with AI:** Uses GPT-4o-mini to read long, messy job descriptions and extract the core truth: *How many years of experience are truly required? Which 5 tech skills matter most? Is English required?*
4. **Organizes the Data Beautifully:** Cleans and arranges everything into an ultra-fast analytical database (DuckDB).
5. **Acts as Your AI Career Coach:** Allows you to simply drop in your PDF Resume/CV. The system compares your profile against hundreds of active openings, finds your best matches, and provides a customized step-by-step career development roadmap.

---

## 📑 Table of Contents
- [1. System Architecture & Real Execution Flow](#1-system-architecture--real-execution-flow)
- [2. The 4-Tier Expired Job Defense System](#2-the-4-tier-expired-job-defense-system)
- [3. Detailed Pipeline Phases](#3-detailed-pipeline-phases)
- [4. Tech Stack & Engineering Highlights](#4-tech-stack--engineering-highlights)
- [5. Project Directory Structure](#5-project-directory-structure)
- [6. Setup & Installation Guide](#6-setup--installation-guide)
- [7. How to Run the Platform](#7-how-to-run-the-platform)

---

## 1. System Architecture & Real Execution Flow

The platform follows a clean **3-Tier Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** with a dedicated **Hybrid Vector Database & RAG Serving Layer**.

<div align="center">
  <a href="docs/assets/architecture.svg">
    <img src="docs/assets/architecture.svg" alt="Lakehouse & RAG Pipeline Architecture" width="100%">
  </a>
</div>

---

## 2. The 4-Tier Expired Job Defense System

Job postings change rapidly: recruiters take down jobs, or listings expire while their URLs still respond. Stale data wastes candidates' time and wastes costly OpenAI tokens. To solve this, the platform uses a **4-Tier Defense System**:

| Layer | Component | Mechanism | Result |
| :--- | :--- | :--- | :--- |
| **Tier 1: Front-Door Extraction** | `enrich_job_details_*.py` | Inspects live HTML DOM selectors for expiration banners (`.job-actions .bg-light-warning-color` on ITViec, `.box-apply-expired` on TopCV). | Dead jobs are marked with `job_description = 'EXPIRED'` before reaching AI. |
| **Tier 2: AI Staging Gatekeeper** | `ai_extractor.py` | Automatically flags any `EXPIRED` jobs as `status = 'Inactive'` and skips LLM API calls. | Skips calling OpenAI, saving 100% of LLM tokens on dead jobs. |
| **Tier 3: Time-to-Live (TTL) & Lifecycle Tracking** | `silver_jobs.sql` | Calculates `days_open = CURRENT_DATE - first_seen_at`. If a job is not re-crawled within **7 days**, it automatically transitions to `status = 'Inactive'`. | Prevents false expirations over weekends while ensuring closed postings naturally lapse. |
| **Tier 4: Adaptive Verification & Qdrant Soft Delete** | `purge_expired_jobs.py` & `sync_qdrant.py` | Targeted crawler checks live HTTP status/DOM banners **only on high-risk jobs** (`days_open >= 7`), reducing network requests by 87%. Inactive jobs are **Soft Deleted** (`is_active = False`) in Qdrant, preventing vector data loss and allowing instant reactivation. | Keeps the Vector database 100% synchronized with active listings without destructive point deletion. |

---

## 3. Detailed Pipeline Phases

### Phase 1: Resilient Scraping & Parquet Landing Zone
- **ITViec Worker (`crawl_data_from_ITVIEC.py` & `enrich_job_details_ITVIEC.py`):**
  Uses `curl_cffi` to impersonate standard browser TLS fingerprints (Chrome 120), bypassing anti-bot blockers and fetching job cards into clean, snappy-compressed Parquet files.
- **TopCV Worker (`crawl_data_from_TOPCV.py` & `enrich_job_details_TOPCV.py`):**
  Uses `SeleniumBase UC (Undetected Chromedriver)` inside an Xvfb virtual display in Docker to handle Cloudflare Turnstile challenges, routing job detail queries through the `FlareSolverr` microservice.
- **Decoupled Landing Zone:**
  Crawlers write files to `data/landing/` without touching the database directly, completely eliminating file locking conflicts.

### Phase 2: Bronze Ingestion & AI Enrichment
- `scripts/ingest_landing_to_bronze.py`:
  Connects to DuckDB for less than 1 second to perform high-speed bulk ingestion (`read_parquet`) into `raw_itviec_jobs` and `raw_topcv_jobs`, initializes `first_seen_at`, and archives processed files to `data/archive/`.
- `scripts/ai_extractor.py`:
  Extracts structured fields using `AsyncOpenAI` and `Instructor` (rate-limited via Semaphore), persisting 100% of extractions into the permanent `raw_ai_extractions` Bronze table.

### Phase 3: Silver Transformation (dbt Core Engine)
- **`silver_jobs.sql`:**
  - Unified deduplication using `ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY crawl_timestamp DESC)`.
  - Normalizes Vietnamese locations (Hà Nội, Hồ Chí Minh, Đà Nẵng, Remote).
  - Automated salary parsing via `parse_salary.sql` macro (`salary_min_vnd`, `salary_max_vnd`, `salary_avg_vnd`).
  - Lifecycle tracking (`first_seen_at`, `days_open`) and 7-day TTL status calculation.
- **`silver_job_skills.sql`:**
  - Unnests AI-extracted JSON skill arrays and joins with `tech_skill_taxonomy.csv` seed.
- **`silver_all_jobs.sql`:**
  - Zero-maintenance backward-compatibility view serving queries seamlessly.

### Phase 4: Gold Analytics Marts & Vector Synchronization
- **Gold Marts (`dbt run --select gold`):**
  - `gold_role_summary`: Aggregate metrics per role.
  - `gold_tech_stack_counts`: Most in-demand technical skills.
  - `gold_tech_stack_by_level`: Tech stack demand across seniority levels.
  - `gold_skill_cooccurrence`: Frequent skill pairs in job listings.
  - `gold_salary_benchmark`: Statistical salary distribution (Min, P25, Median, P75, Max, Avg) by Role and Level.
- **Data Quality Contracts (`dbt test`):**
  - Enforces uniqueness, non-null values, and accepted value sets before vector sync.
- **Qdrant Vector DB Soft-Delete Sync (`scripts/sync_qdrant.py`):**
  - Applies `is_active = False` payload to inactive jobs without destructive deletions.
  - Generates Dense Embeddings (`text-embedding-3-small`) and Sparse BM25 Embeddings (`FastEmbedSparse`) for **Hybrid Vector Search**.

### Phase 5: Streamlit Serving & AI Career Coach
- **Tab 1 - Market Intelligence Dashboard:**
  - Macro Market Overview (Market Structure, Job Levels, Required Experience).
  - Dynamic Role-Specific Deep Dive (Filterable Tech Stack & English Demand).
  - **Tech Salary Benchmark** (Interactive salary distribution charts and summary table from `gold_salary_benchmark`).
- **Tab 2 - AI Career Coach (Two-Stage Enterprise RAG):**
  1. Candidate uploads a resume in PDF format.
  2. `PyPDFLoader` extracts text, and GPT-4o-mini determines practical experience (Years of Experience), Target Role, and Primary Tech Stack.
  3. Smart Gatekeeper & Soft Scoring matches candidates against active listings.
  4. Local Cross-Encoder Reranker (`BAAI/bge-reranker-base`) reranks the Top matching fits.
  5. Rich job cards display Company Name, Source, `days_open` freshness badge, and disclosed salary.
  6. GPT-4o-mini generates a comprehensive skills gap analysis and a personalized 30-day preparation roadmap.

---

## 4. Tech Stack & Engineering Highlights

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.8.1 | CeleryExecutor with Redis & PostgreSQL for robust scheduling |
| **Data Lakehouse** | DuckDB | Columnar OLAP engine for fast aggregation and transformation |
| **Transformation** | dbt (data build tool) | 3-Tier Medallion architecture (Silver $\rightarrow$ Gold) + data contracts & testing |
| **Vector Engine** | Qdrant Local | Hybrid Search (Dense 1536-dim + BM25 Sparse Vectors) with Soft Delete |
| **LLM & Structuring** | OpenAI GPT-4o-mini + Instructor | Async structured information extraction via Pydantic v2 schemas |
| **Reranking** | HuggingFace BAAI/bge-reranker-base | Cross-Encoder for high-precision CV-to-job matching |
| **Web Scraping** | curl_cffi, SeleniumBase, FlareSolverr | Anti-bot bypass, TLS fingerprinting, and Cloudflare challenge solving |
| **User Interface** | Streamlit + Plotly Express | Interactive analytical dashboard with salary benchmarks & AI Career Coach |

---

## 5. Project Directory Structure

```text
de-job-market-tracker/
├── .env                              # Environment variables (OpenAI API key, cookies, paths)
├── docker-compose.yaml               # Complete stack (Airflow, PostgreSQL, Redis, FlareSolverr)
├── Dockerfile                        # Custom Airflow image with Chrome, Xvfb & PyArrow
├── requirements.txt                  # Consolidated Python dependencies
├── app.py                            # Streamlit Dashboard & AI Career Coach application
├── job_market.duckdb                 # DuckDB database (Bronze, Silver, Gold layers)
├── analytics_dbt/                    # dbt transformation project
│   ├── dbt_project.yml               # dbt configuration
│   ├── profiles.yml                  # Cross-platform profile (Local & Docker)
│   ├── macros/
│   │   └── parse_salary.sql          # Automated VND salary regex & unit conversion macro
│   ├── seeds/
│   │   └── tech_skill_taxonomy.csv   # Canonical tech skill categorization seed
│   └── models/
│       ├── silver/                   # Unified silver models (silver_jobs, silver_job_skills, silver_all_jobs)
│       └── gold/                     # Analytics marts (gold_salary_benchmark, gold_role_summary, etc.)
├── dags/
│   └── it_job_pipeline.py            # Airflow DAG definition (Runs daily at 07:00 AM VN)
├── data/
│   ├── landing/                      # Landing zone (Temporary Parquet batches)
│   │   ├── itviec/
│   │   └── topcv/
│   └── archive/                      # Historical archive of ingested Parquet files
├── local_qdrant_db/                  # Persistent local Qdrant vector storage
└── scripts/
    ├── crawl_data_from_ITVIEC.py     # ITViec list crawler (curl_cffi)
    ├── enrich_job_details_ITVIEC.py  # ITViec JD details enricher & expiration check
    ├── crawl_data_from_TOPCV.py      # TopCV list crawler (SeleniumBase UC)
    ├── enrich_job_details_TOPCV.py   # TopCV JD details enricher (FlareSolverr)
    ├── ingest_landing_to_bronze.py   # Parquet Landing Zone to DuckDB Bronze Ingestor
    ├── ai_extractor.py               # Async LLM Structured Data Extractor (writes to raw_ai_extractions)
    ├── cleanup_jobs.py               # Silver layer audit & statistics reporter
    ├── purge_expired_jobs.py         # Adaptive URL verification (days_open >= 7) & Soft Delete trigger
    └── sync_qdrant.py                # Safe Hybrid Vector Sync with Soft Delete (is_active=False)
```

---

## 6. Setup & Installation Guide

### Prerequisites
- [Docker Desktop](https://www.docker.com/) (allocated at least 4 GB RAM)
- Python 3.10+ (for local UI and development)

### 1. Configure Environment Variables
Create a `.env` file in the project root:
```ini
OPENAI_API_KEY=your_openai_api_key_here
ITVIEC_COOKIE=your_itviec_cookie_if_available
TOPCV_COOKIE=your_topcv_cookie_if_available
FLARESOLVERR_URL=http://flaresolverr:8191/v1
DUCKDB_PATH=/opt/airflow/job_market.duckdb
```

### 2. Local Python Environment (For Running Streamlit & dbt Locally)
```bash
# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\activate       # Windows
# source venv/bin/activate    # macOS / Linux

# Install dependencies
pip install -r requirements.txt
```

---

## 7. How to Run the Platform

### Method A: Automated Orchestration via Apache Airflow (Recommended)

1. **Start the Docker Cluster:**
   ```bash
   docker compose up -d --build
   ```
2. **Access Airflow UI:**
   - URL: [http://localhost:8080](http://localhost:8080)
   - Credentials: `airflow` / `airflow`
3. **Trigger Pipeline:**
   - Unpause DAG **`it_job_market_etl_pipeline`**.
   - The DAG runs automatically every day at **07:00 AM (Asia/Ho_Chi_Minh)**, or can be triggered manually at any time.
4. **Launch Streamlit UI:**
   ```bash
   streamlit run app.py
   ```
   - Open [http://localhost:8501](http://localhost:8501) in your browser.

> [!NOTE]
> **Qdrant Storage Lock:** The local Qdrant vector database uses file-level locking. If you run manual maintenance (`python scripts/sync_qdrant.py`), stop Streamlit first (`Ctrl+C`), run the sync, and then restart Streamlit.

---

### Method B: Manual Step-by-Step Execution (Local Testing)

If you wish to test individual components outside of Docker:

```bash
# 1. Crawl and enrich new jobs
python scripts/crawl_data_from_ITVIEC.py
python scripts/enrich_job_details_ITVIEC.py
python scripts/crawl_data_from_TOPCV.py
python scripts/enrich_job_details_TOPCV.py

# 2. Ingest landing Parquet files into DuckDB Bronze
python scripts/ingest_landing_to_bronze.py

# 3. Extract structured fields with AI into raw_ai_extractions
python scripts/ai_extractor.py itviec
python scripts/ai_extractor.py topcv

# 4. Transform through dbt Silver & Gold layers
cd analytics_dbt
dbt seed
dbt run --select silver
dbt run --select gold
dbt test
cd ..

# 5. Sync active jobs into Qdrant Vector DB (Soft Delete enabled)
python scripts/sync_qdrant.py

# 6. (Optional) Run Adaptive Expiration Verification for high-risk jobs (days_open >= 7)
python scripts/purge_expired_jobs.py

# 7. Start the Web Dashboard & AI Career Coach
streamlit run app.py
```

---

## 🛡️ License
This project is open-source under the [MIT License](LICENSE). Built for educational, analytical, and career advancement purposes.
