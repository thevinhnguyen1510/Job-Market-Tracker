# 🚀 IT Job Market Tracker & AI Career Coach

**An End-to-End Data Engineering Pipeline for Market Intelligence & Enterprise RAG-driven Career Coaching.**

This project is a comprehensive system designed to crawl, process, and analyze the IT job market in Vietnam (specifically ITViec & TopCV). It leverages a modern Data Stack to provide real-time market insights and an AI-powered Career Coach that performs deep CV-to-Job matching using RAG (Retrieval-Augmented Generation).

---

## 🏗️ System Architecture

The system is built on a robust, modern 4-tier data architecture. Below is the high-level workflow of the pipeline:

```mermaid
graph TD
    subgraph 1. Ingestion Layer
        A[ITViec] -->|Asymmetric Scraping & TLS Fingerprinting| C(Python Crawlers)
        B[TopCV] -->|Asymmetric Scraping & TLS Fingerprinting| C
    end

    subgraph 2. Processing & Storage Layer
        C -->|Raw Data| D[(DuckDB)]
        D -->|Staging & Intermediate| E{dbt: Silver Layer}
        E -->|Unstructured Text| F[OpenAI API Enrichment]
        F -->|Structured Entities| D
        D -->|Aggregations| G{dbt: Gold Layer}
    end

    subgraph 3. AI Data Preparation
        G -->|Incremental Sync| H[(Qdrant Vector DB)]
    end

    subgraph 4. Application Layer
        G -->|Metrics| I[Streamlit Dashboard / Plotly]
        H -->|Hybrid Search & Reranker| J[AI Career Coach / RAG]
    end

    %% Airflow Orchestration
    K((Apache Airflow <br> Orchestrator)) -.- C
    K -.- E
    K -.- F
    K -.- G
    K -.- H

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
