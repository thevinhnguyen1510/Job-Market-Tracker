Here is a professional, high-impact **English version** of your `README.md`. I’ve polished the technical terminology to ensure it sounds like a top-tier Engineering project.

---

# 🚀 IT Job Market Tracker & AI Career Coach

**An End-to-End Data Engineering Pipeline for Market Intelligence & Enterprise RAG-driven Career Coaching.**

This project is a comprehensive system designed to crawl, process, and analyze the IT job market in Vietnam (specifically ITViec & TopCV). It leverages a modern Data Stack to provide real-time market insights and an AI-powered Career Coach that performs deep CV-to-Job matching using RAG (Retrieval-Augmented Generation).

---

## 🏗️ System Architecture

The system is built on a robust 4-layer architecture:

### 1. Data Ingestion Layer
* **Sources:** ITViec & TopCV.
* **Tech:** Python (`curl_cffi`, `requests`).
* **Feature:** Implements **Asymmetric Scraping** and TLS Fingerprinting to bypass Cloudflare Turnstile anti-bot systems on TopCV with high efficiency.

### 2. Data Processing & Storage Layer
* **Orchestration:** **Apache Airflow**. Manages and monitors daily automated ETL pipelines.
* **Data Warehouse:** **DuckDB**. A high-performance analytical database using columnar storage.
* **Hybrid Transformation Strategy:** * **Python AI Parsing:** Leverages LLMs to parse complex, unstructured Job Descriptions into structured data (Tech Stack, Years of Experience (YOE), and English requirements).
    * **dbt (Data Build Tool):** Implements a **Medallion Architecture**-inspired modeling flow:
        * **Staging:** Standardizes raw data from various sources (`stg_itviec_jobs`, `stg_topcv_jobs`).
        * **Intermediate:** Joins and unions multi-source data into a unified schema (`int_all_jobs`).
        * **Marts (Gold):** Aggregates business-ready metrics (`gold_tech_stack_counts`, `gold_role_summary`) for the Dashboard.
* **Data Maintenance:** Automated `cleanup_jobs.py` script to identify and mark "Inactive" expired postings.

### 3. AI Data Preparation (Vector DB)
* **Technology:** **Qdrant** (Vector Database).
* **Sync Logic:** Daily incremental synchronization from DuckDB to Qdrant.
* **Search Engine:** Implements **Hybrid Search** combining Dense Vectors (OpenAI Embeddings) and Sparse Vectors (BM25) for superior retrieval accuracy.

### 4. Application Layer
* **Framework:** **Streamlit**.
* **Dashboard:** Interactive market intelligence visualized via **Plotly** (Top Tech Stacks, Job Levels, Language Requirements).
* **AI Career Coach:** An advanced RAG pipeline that integrates a **Cross-Encoder Reranker** (`BAAI/bge-reranker-base`) for precise CV-to-JD matching and Gap Analysis.

---

## 🛠️ Tech Stack

| Category | Tools |
| :--- | :--- |
| **Languages** | Python 3.10+ |
| **Data Engineering** | Apache Airflow, DuckDB, dbt, Pandas |
| **AI & RAG** | OpenAI (GPT-4o-mini), Langchain, Qdrant, HuggingFace (Reranker) |
| **Frontend/Viz** | Streamlit, Plotly |
| **Infrastructure** | Docker, Docker Compose |

---

## 📂 Project Structure

```text
├── .airflow/              # Airflow configuration & docker-compose
├── dags/                  # Airflow DAGs for pipeline orchestration
├── dbt/                   # dbt Project (Staging, Intermediate, Marts models)
├── scripts/               # Scrapers, Sync scripts, and AI Parsing logic
├── local_qdrant_db/       # Local Vector Database storage
├── job_market.duckdb      # Local Analytical Database (DuckDB)
├── app.py                 # Main Streamlit application
├── .env                   # Configuration & API Keys
└── requirements.txt       # Python dependencies
```

---

## 🚀 Getting Started

### 1. Prerequisites
* Python 3.10+
* Docker & Docker Compose.
* OpenAI API Key.

### 2. Installation
```bash
# Clone the repository
git clone https://github.com/your-username/it-job-market-tracker.git
cd it-job-market-tracker

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration
Create a `.env` file in the root directory:
```text
OPENAI_API_KEY=your_openai_key_here
HF_TOKEN=your_huggingface_token_here
DBT_PROFILES_DIR=./dbt
```

### 4. Running the System
1.  **Start Airflow:** `docker-compose up -d`
2.  **Sync Vector DB:** `python scripts/sync_qdrant.py`
3.  **Run Streamlit App:** `streamlit run app.py`

---

## 📊 AI Coach Key Features

* **CV Parsing:** Automatically extracts YOE and Tech Stack from PDF documents.
* **Deep Semantic Search:** Scans 300+ live jobs to find the most relevant opportunities.
* **Cross-Encoder Reranking:** Validates matches using local SOTA reranker models for high-fidelity ranking.
* **Gap Analysis:** Provides actionable feedback on missing skills and a 30-day roadmap for candidates.

---

## 🤝 Contributing
Feel free to open an issue or submit a pull request if you have ideas for improvement!

**Author:** Nguyen The Vinh - Data Engineer.
