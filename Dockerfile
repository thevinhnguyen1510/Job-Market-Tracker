FROM apache/airflow:2.8.1-python3.11

# ==========================================
# 1. SYSTEM DEPENDENCIES 
# ==========================================
USER root
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    chromium \
    chromium-driver \
    xvfb \
    && apt-get clean

# ==========================================
# 2. PYTHON DEPENDENCIES 
# ==========================================
USER airflow
RUN pip install --no-cache-dir --upgrade pip

# Khai báo biến môi trường để trỏ đúng vào danh sách thư viện gốc của Airflow 2.8.1
ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# BƯỚC 1: Cài đặt Core Airflow & Celery với Constraint nghiêm ngặt
RUN pip install --no-cache-dir --prefer-binary \
    "apache-airflow==${AIRFLOW_VERSION}" \
    celery==5.3.6 \
    click==8.1.7 \
    --constraint "${CONSTRAINT_URL}"

# BƯỚC 2: Cài đặt các thư viện Data/Scraping/AI bên ngoài (không dùng Constraint)
RUN pip install --no-cache-dir --prefer-binary \
    duckdb \
    openai \
    python-dotenv \
    requests \
    beautifulsoup4 \
    pandas \
    curl-cffi \
    "seleniumbase>=4.25.0" \
    instructor \
    dbt-duckdb \
    click==8.1.7 \
    qdrant-client \
    langchain-core \
    langchain \
    langchain-openai \
    langchain-qdrant \
    fastembed