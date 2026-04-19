FROM apache/airflow:2.8.1-python3.11

# ==========================================
# 1. SYSTEM DEPENDENCIES 
# ==========================================
USER root

RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    xvfb \
    x11-utils \
    python3-tk \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libnss3 \
    libcups2 \
    libxss1 \
    libxrandr2 \
    libasound2 \
    libatk1.0-0 \
    libgtk-3-0 \
    && apt-get clean

RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

RUN touch /home/airflow/.Xauthority && \
    chown airflow:root /home/airflow/.Xauthority && \
    chmod 600 /home/airflow/.Xauthority

# ==========================================
# 2. PYTHON DEPENDENCIES 
# ==========================================
USER airflow
RUN pip install --no-cache-dir --upgrade pip

ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir --prefer-binary \
    "apache-airflow==${AIRFLOW_VERSION}" \
    celery==5.3.6 \
    click==8.1.7 \
    --constraint "${CONSTRAINT_URL}"

RUN pip install --no-cache-dir --prefer-binary \
    duckdb \
    openai \
    python-dotenv \
    requests \
    beautifulsoup4 \
    pandas \
    curl-cffi \
    "seleniumbase>=4.25.0" \
    sbvirtualdisplay \
    instructor \
    dbt-duckdb \
    click==8.1.7 \
    qdrant-client \
    langchain-core \
    langchain \
    langchain-openai \
    langchain-qdrant \
    fastembed \
    pyautogui