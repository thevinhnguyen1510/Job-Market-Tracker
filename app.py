import streamlit as st
import duckdb
import pandas as pd
import os
import tempfile
import re
from dotenv import load_dotenv
import plotly.express as px

# --- LANGCHAIN & QDRANT LIBRARIES ---
from langchain_community.document_loaders import PyPDFLoader
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, Range
from langchain_core.documents import Document
from qdrant_client.models import VectorParams, Distance

# 1. SETUP & CONFIGURATION
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
QDRANT_PATH = "local_qdrant_db" # Path to store Qdrant Vector DB
COLLECTION_NAME = "it_jobs_collection"

st.set_page_config(page_title="IT Job Market & AI Coach", layout="wide", page_icon="🚀")
st.title("🚀 IT Job Market Tracker & AI Career Coach")

tab1, tab2 = st.tabs(["📊 Market Dashboard", "🤖 AI Career Coach (RAG)"])

# ==========================================
# TAB 1: DASHBOARD (EXECUTIVE LEVEL)
# ==========================================
with tab1:
    st.markdown("### 📊 IT Market Intelligence Dashboard")
    st.markdown("Data is automatically extracted, standardized, and visualized directly from the **Silver Data Layer**.")
    
    conn = duckdb.connect('job_market.duckdb', read_only=True)
    try:
        # ==========================================
        # SECTION 1: MACRO OVERVIEW (STATIC)
        # ==========================================
        st.markdown("#### 🌍 1. Macro Market Overview (Market Structure)")
        
        # Metrics total 
        macro_jobs = conn.execute("SELECT COUNT(*) FROM silver_itviec_jobs WHERE job_level != 'Error'").fetchone()[0]
        macro_yoe = conn.execute("SELECT ROUND(AVG(min_years_of_experience), 1) FROM silver_itviec_jobs WHERE min_years_of_experience IS NOT NULL").fetchone()[0]
        
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("Total Jobs Scanned", f"{macro_jobs:,}")
        col_m2.metric("Market Avg. Experience", f"{macro_yoe} Yrs")
        col_m3.metric("Data Engine", "DuckDB + dbt")
        
        # Macro charts (Static, not filtered)
        macro_col1, macro_col2 = st.columns(2)
        
        with macro_col1:
            df_levels = conn.execute("""
                SELECT job_level, COUNT(*) as count 
                FROM silver_itviec_jobs 
                WHERE job_level != 'Error' AND job_level IS NOT NULL 
                GROUP BY job_level
            """).df()
            if not df_levels.empty:
                fig_donut = px.pie(
                    df_levels, values='count', names='job_level', hole=0.4, 
                    title="Market Structure: Job Levels", 
                    color_discrete_sequence=px.colors.sequential.Teal
                )
                fig_donut.update_traces(textposition='inside', textinfo='percent+label')
                fig_donut.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0))
                st.plotly_chart(fig_donut, use_container_width=True)
                
        with macro_col2:
            df_yoe_macro = conn.execute("""
                SELECT min_years_of_experience 
                FROM silver_itviec_jobs 
                WHERE min_years_of_experience IS NOT NULL
            """).df()
            if not df_yoe_macro.empty:
                # New insight: Experience distribution histogram
                fig_hist = px.histogram(
                    df_yoe_macro, x="min_years_of_experience", nbins=10, 
                    title="Market Structure: Required Experience", 
                    color_discrete_sequence=['#ff4b4b']
                )
                fig_hist.update_layout(xaxis_title="Years of Experience", yaxis_title="Number of Jobs", margin=dict(t=40, b=0, l=0, r=0))
                st.plotly_chart(fig_hist, use_container_width=True)

        st.divider()

        # ==========================================
        # SECTION 2: DEEP DIVE (DYNAMIC FILTERED)
        # ==========================================
        st.markdown("#### 🔬 2. Role-Specific Deep Dive (Filterable)")
        
        df_levels_unique = conn.execute("SELECT DISTINCT job_level FROM silver_itviec_jobs WHERE job_level != 'Error' AND job_level IS NOT NULL ORDER BY job_level").df()
        levels_list = df_levels_unique['job_level'].tolist() if not df_levels_unique.empty else []
        
        selected_level = st.selectbox(
            "🎯 Select a Job Level to deeply analyze its specific requirements:", 
            ["All Levels"] + levels_list,
            key="job_level_filter" 
        )
        
        global_filter_sql = ""
        if selected_level != "All Levels":
            global_filter_sql = f"AND job_level = '{selected_level}'"

        # Count jobs and calculate average years of experience
        dyn_jobs = conn.execute(f"SELECT COUNT(*) FROM silver_itviec_jobs WHERE job_level != 'Error' {global_filter_sql}").fetchone()[0]
        dyn_yoe = conn.execute(f"SELECT ROUND(AVG(min_years_of_experience), 1) FROM silver_itviec_jobs WHERE min_years_of_experience IS NOT NULL {global_filter_sql}").fetchone()[0]
        dyn_yoe = dyn_yoe if dyn_yoe is not None else 0
        
        # Calculate percentage of jobs WITH English requirement (excluding "Not mentioned")
        eng_demand = conn.execute(f"SELECT COUNT(*) FROM silver_itviec_jobs WHERE english_requirement != 'Not mentioned' AND english_requirement IS NOT NULL {global_filter_sql}").fetchone()[0]
        eng_pct = round((eng_demand / dyn_jobs) * 100, 1) if dyn_jobs > 0 else 0

        # Update 3 Metric cards
        col_d1, col_d2, col_d3 = st.columns(3)
        col_d1.metric(f"Jobs ({selected_level})", f"{dyn_jobs:,}")
        col_d2.metric("Avg. Experience", f"{dyn_yoe} Yrs")
        col_d3.metric("English Required", f"{eng_pct}%", help="Percentage of JDs requiring English (from Intermediate level up)")

        dyn_col1, dyn_col2 = st.columns([1.5, 1])
        
        with dyn_col1:
            # Chart 1: TECH STACK (Keep original)
            df_skills = conn.execute(f"""
                WITH cleaned_strings AS (
                    SELECT job_url, REPLACE(REPLACE(REPLACE(ai_core_tech_stack, '[', ''), ']', ''), '"', '') AS clean_stack
                    FROM silver_itviec_jobs 
                    WHERE ai_core_tech_stack IS NOT NULL {global_filter_sql}
                ),
                unnested_skills AS (
                    SELECT job_url, TRIM(UNNEST(string_split(clean_stack, ','))) AS skill
                    FROM cleaned_strings WHERE clean_stack != ''
                )
                SELECT skill, COUNT(DISTINCT job_url) AS total_mentions
                FROM unnested_skills 
                WHERE skill != '' 
                GROUP BY skill 
                ORDER BY total_mentions DESC LIMIT 10
            """).df()

            if not df_skills.empty:
                fig_bar = px.bar(
                    df_skills, x='total_mentions', y='skill', orientation='h',
                    title=f"Top 10 Tech Stack for {selected_level}",
                    color='total_mentions', color_continuous_scale='Reds'
                )
                fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(t=40, b=0, l=0, r=0))
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("No tech stack data available.")

        with dyn_col2:
            # Chart 2: ENGLISH COMPETENCY (Based on new data classified by LLM)
            df_eng_levels = conn.execute(f"""
                SELECT english_requirement, COUNT(*) as count 
                FROM silver_itviec_jobs 
                WHERE english_requirement IS NOT NULL {global_filter_sql}
                GROUP BY english_requirement
            """).df()
            
            if not df_eng_levels.empty:
                # Change to Bar Chart vertical for easier comparison of levels
                fig_eng = px.bar(
                    df_eng_levels, x='english_requirement', y='count',
                    title=f"English Proficiency Demand",
                    color='english_requirement',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_eng.update_layout(
                    xaxis_title="", 
                    yaxis_title="Number of Jobs", 
                    showlegend=False,
                    xaxis_tickangle=-45,
                    margin=dict(t=40, b=0, l=0, r=0)
                )
                st.plotly_chart(fig_eng, use_container_width=True)

    except Exception as e:
        st.error(f"Dashboard query error: {e}")
    finally:
        conn.close()

# ==========================================
# TAB 2: AI CAREER COACH (RAG SYSTEM WITH QDRANT)
# ==========================================
with tab2:
    st.subheader("🎯 Upload your CV & Get AI Gap Analysis")
    st.markdown("Strict Verification Pipeline: **Education -> Experience -> Tech Stack**.")
    
    uploaded_cv = st.file_uploader("Upload CV (PDF format only)", type=["pdf"])
    
    if uploaded_cv is not None:
        if st.button("Analyze CV & Match Jobs", type="primary"):
            
            if not OPENAI_API_KEY:
                st.error("⚠️ Missing OPENAI_API_KEY. Please check your .env file!")
                st.stop()

            with st.status("AI is processing...", expanded=True) as status:
                try:
                    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0)
                    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

                    # STEP 1: READ PDF FILE
                    status.update(label="1. Loading and parsing CV document...")
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                        tmp_file.write(uploaded_cv.getvalue())
                        tmp_path = tmp_file.name
                    
                    loader = PyPDFLoader(tmp_path)
                    cv_pages = loader.load_and_split()
                    cv_text = " ".join([p.page_content for p in cv_pages])
                    os.remove(tmp_path) 

                    # STEP 1.5: QUERY TRANSFORMATION (Estimate YOE & Translate to English Query)
                    status.update(label="2. Analyzing practical experience & Standardizing query...")
                    search_profile_prompt = f"""
                    Read the following CV and perform 2 tasks:
                    1. Count the total years of practical work experience (return an integer). Do not include university study time.
                    2. Summarize the candidate's Job Role and core Tech Stack into a SINGLE, highly relevant English sentence (e.g., 'Senior Backend Developer skilled in Python, Django, AWS, and PostgreSQL').

                    Return the result EXACTLY in the following format (Do not add any other text):
                    YOE: [Number of years]
                    QUERY: [English summary query]

                    CV Text: {cv_text[:3000]}
                    """
                    
                    profile_response = llm.invoke(search_profile_prompt).content
                    
                    # Extract Results
                    candidate_yoe = 0
                    search_query = cv_text # Default fallback
                    
                    try:
                        lines = profile_response.strip().split('\n')
                        for line in lines:
                            if line.startswith("YOE:"):
                                candidate_yoe = int(re.findall(r'\d+', line)[0])
                            elif line.startswith("QUERY:"):
                                search_query = line.replace("QUERY:", "").strip()
                    except Exception as e:
                         st.warning("Error extracting YOE. Defaulting to 0 years.")
                    
                    st.write(f"*(AI estimated experience: **{candidate_yoe} years**)*")
                    st.write(f"*(Optimized Search Query: **{search_query}**)*")

                    # STEP 2: LOAD OR INITIALIZE QDRANT VECTOR DATABASE
                    status.update(label="3. Accessing Qdrant Vector Database...")
                    
                    # Use Streamlit Cache to ensure only ONE connection is made
                    @st.cache_resource
                    def get_qdrant_client():
                        return QdrantClient(path=QDRANT_PATH)
                    
                    client = get_qdrant_client()
                    
                    # CHECK: Collection already exists?
                    if client.collection_exists(collection_name=COLLECTION_NAME):
                        # Load from Local Cache
                        status.update(label="3. Loading Qdrant DB from local storage...")
                        vectorstore = QdrantVectorStore(
                            client=client, 
                            collection_name=COLLECTION_NAME, 
                            embedding=embeddings
                        )
                    else:
                        # First time setup: Generate and Save to Qdrant
                        status.update(label="3. No cache found. Initializing Qdrant DB (First run)...")
                        conn = duckdb.connect('job_market.duckdb', read_only=True)
                        query = f"""
                            SELECT job_url, job_title, ai_core_tech_stack, min_years_of_experience, english_requirement, job_level 
                            FROM silver_itviec_jobs 
                            WHERE job_level != 'Error' LIMIT 200
                        """
                        jobs_df = conn.execute(query).df()
                        conn.close()

                        if jobs_df.empty:
                            st.warning("Database is empty!")
                            st.stop()

                        job_docs = []
                        for _, row in jobs_df.iterrows():
                            content = f"Title: {row['job_title']} | Tech: {row['ai_core_tech_stack']} | Level: {row['job_level']} | Exp: {row['min_years_of_experience']} years | English: {row['english_requirement']}"
                            job_docs.append(Document(
                                page_content=content,
                                metadata={
                                    "job_title": row['job_title'], 
                                    "job_url": row['job_url'], 
                                    "yoe": row['min_years_of_experience']
                                }
                            ))

                        #Calculate vector dimension and create collection manually
                        #vector_size = len(embeddings.embed_query("test")) # AI automatically calculates 1536 dimensions
                        client.create_collection(
                            collection_name=COLLECTION_NAME,
                            vectors_config=VectorParams(size=1536, distance=Distance.COSINE)
                        )
                        
                        # Initialize VectorStore and inject data (Absolutely no file locking)
                        vectorstore = QdrantVectorStore(
                            client=client,
                            collection_name=COLLECTION_NAME,
                            embedding=embeddings
                        )
                        vectorstore.add_documents(job_docs)
                    
                    # STEP 3: SEARCH WITH STANDARDIZED QUERY & QDRANT PRE-FILTER
                    status.update(label="4. Finding best candidates using Qdrant Pre-filtering...")
                    
                    # Qdrant Native Metadata Filter: Job YOE <= Candidate YOE + 1
                    qdrant_filter = Filter(
                        must=[
                            FieldCondition(
                                key="metadata.yoe", 
                                range=Range(lte=candidate_yoe + 1)
                            )
                        ]
                    )
                    
                    # Search using the English transformed query + Qdrant Filter
                    retriever = vectorstore.as_retriever(
                        search_kwargs={"k": 3, "filter": qdrant_filter} 
                    )
                    matched_jobs = retriever.invoke(search_query)
                    
                    if not matched_jobs:
                        st.warning(f"Unfortunately, no jobs found matching {candidate_yoe} years of experience.")
                        st.stop()
                        
                    jobs_context = "\n\n".join([f"Job {i+1}: {doc.page_content}" for i, doc in enumerate(matched_jobs)])

                    # STEP 4: GENERATE HR EVALUATION REPORT
                    status.update(label="5. HR Expert is drafting the Gap Analysis...")
                    llm_eval = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
                    prompt = f"""
                    You are a highly analytical and strict HR Director in the IT industry.
                    Below is the candidate's CV and the Top 3 matching jobs (already pre-filtered by experience level).

                    ### CANDIDATE'S ORIGINAL CV:
                    {cv_text[:3500]} 

                    ### TOP 3 MATCHING JOBS:
                    {jobs_context}

                    ### EVALUATION TASKS (STRICTLY FOLLOW THIS ORDER):
                    1. **Background Assessment (Education & Learning):** Briefly comment on the candidate's university, major, certificates, or self-learning path. Is this foundation solid for long-term growth?
                    2. **Experience Assessment (Work History):** How many years of experience does the candidate have? Are the projects deep enough or superficial? Compared to the required experience of the 3 Jobs above, is the candidate underqualified?
                    3. **Tech Stack Assessment (Tools & Frameworks):** Point out exactly which technical skills the candidate is strong at, and which required skills from the Jobs are missing (The Gap).
                    4. **30-Day Strategy:** Provide the most practical advice for the candidate to bridge the Gap before applying to these 3 companies.

                    Respond in English, use clear Markdown formatting, and maintain a direct, professional, and uncompromising tone (do not flatter the candidate).
                    """
                    
                    response = llm_eval.invoke(prompt)
                    status.update(label="Analysis Complete!", state="complete")
                    
                    # RENDER RESULTS
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.info("📌 **Top 3 Best Matching Jobs:**")
                        for job in matched_jobs:
                            title = job.metadata.get("job_title", "View Job Details")
                            url = job.metadata.get("job_url", "#")
                            
                            st.markdown(f"**[{title}]({url})**")
                            clean_content = job.page_content.replace(f"Title: {title} | ", "")
                            st.caption(clean_content)
                            st.divider()
                            
                    with col2:
                        st.markdown(response.content)

                except Exception as e:
                    status.update(label="An error occurred!", state="error")
                    st.error(f"System Error: {e}")