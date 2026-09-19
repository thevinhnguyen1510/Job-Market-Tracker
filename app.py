import warnings
import streamlit as st
import duckdb
import pandas as pd
import os
import tempfile
import re
import hashlib
import uuid
from dotenv import load_dotenv
import plotly.express as px

# --- LANGCHAIN CORE & COMMUNITY ---
from langchain_core.documents import Document
from langchain_community.document_loaders import PyPDFLoader
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

# --- QDRANT CORE ---
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, Range, VectorParams, Distance, SparseVectorParams

# --- QDRANT HYBRID SEARCH ---
from langchain_qdrant import QdrantVectorStore, FastEmbedSparse, RetrievalMode

# --- RERANKER ---
from langchain_classic.retrievers import ContextualCompressionRetriever
from langchain_classic.retrievers.document_compressors import CrossEncoderReranker
from langchain_community.cross_encoders import HuggingFaceCrossEncoder

# ==========================================
# 0. SUPPRESS ANNOYING WARNINGS
# ==========================================
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", module="transformers")

# ==========================================
# 1. SETUP & CONFIGURATION
# ==========================================
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
QDRANT_PATH = "local_qdrant_db" 
COLLECTION_NAME = "all_it_jobs_v6"

st.set_page_config(page_title="IT Job Market & AI Coach", layout="wide", page_icon="🚀")

# ==========================================
# 2. CACHE AI MODELS (CRUCIAL FOR SPEED)
# ==========================================
@st.cache_resource(show_spinner=False)
def load_embedding_models():
    dense = OpenAIEmbeddings(model="text-embedding-3-small")
    sparse = FastEmbedSparse(model_name="Qdrant/bm25")
    return dense, sparse

@st.cache_resource(show_spinner="Loading Cross-Encoder Reranker (~450MB)...")
def get_reranker_model():
    return HuggingFaceCrossEncoder(model_name="BAAI/bge-reranker-base")

# Only load lightweight embeddings globally so dashboard loads instantly
embeddings, sparse_embeddings = load_embedding_models()

st.title("🚀 IT Job Market Tracker & AI Career Coach")

tab1, tab2 = st.tabs(["📊 Market Dashboard", "🤖 AI Career Coach (RAG)"])

# ==========================================
# TAB 1: DASHBOARD (EXECUTIVE LEVEL)
# ==========================================
with tab1:
    st.markdown("### 📊 IT Market Intelligence Dashboard")
    st.markdown("Data is automatically extracted, standardized, and visualized directly from the **Silver Data Layer** (ITviec & TopCV).")
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
    conn = duckdb.connect(db_path, read_only=True)
    try:
        # ==========================================
        # SECTION 1: MACRO OVERVIEW (STATIC)
        # ==========================================
        st.markdown("#### 🌍 1. Macro Market Overview (Market Structure)")
        
        macro_jobs = conn.execute("SELECT COUNT(*) FROM silver_all_jobs WHERE job_level != 'Error' AND status = 'Active'").fetchone()[0]
        macro_yoe = conn.execute("SELECT ROUND(AVG(min_years_of_experience), 1) FROM silver_all_jobs WHERE min_years_of_experience IS NOT NULL AND status = 'Active'").fetchone()[0]
        macro_yoe = macro_yoe if macro_yoe is not None else 0 
        
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("Total Jobs Scanned", f"{macro_jobs:,}")
        col_m2.metric("Market Avg. Experience", f"{macro_yoe} Yrs")
        col_m3.metric("Data Sources", "ITviec & TopCV")
        
        macro_col1, macro_col2 = st.columns(2)
        
        with macro_col1:
            df_levels = conn.execute("""
                SELECT job_level, COUNT(*) as count 
                FROM silver_all_jobs 
                WHERE job_level != 'Error' AND job_level IS NOT NULL AND status = 'Active'
                GROUP BY job_level
                ORDER BY count DESC
            """).df()
            if not df_levels.empty:
                fig_donut = px.pie(
                    df_levels, values='count', names='job_level', hole=0.4, 
                    title="Market Structure: Job Levels", 
                    color_discrete_sequence=px.colors.sequential.Teal
                )
                fig_donut.update_traces(textposition='inside', textinfo='percent+label')
                fig_donut.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0))
                # Fixed Streamlit warning here
                st.plotly_chart(fig_donut, width="stretch")
                
        with macro_col2:
            df_yoe_macro = conn.execute("""
                SELECT min_years_of_experience 
                FROM silver_all_jobs 
                WHERE min_years_of_experience IS NOT NULL AND status = 'Active'
            """).df()
            if not df_yoe_macro.empty:
                fig_hist = px.histogram(
                    df_yoe_macro, x="min_years_of_experience", nbins=10, 
                    title="Market Structure: Required Experience", 
                    color_discrete_sequence=['#ff4b4b']
                )
                fig_hist.update_layout(xaxis_title="Years of Experience", yaxis_title="Number of Jobs", margin=dict(t=40, b=0, l=0, r=0))
                # Fixed Streamlit warning here
                st.plotly_chart(fig_hist, width="stretch")

        st.divider()

        # ==========================================
        # SECTION 2: DEEP DIVE (DYNAMIC FILTERED)
        # ==========================================
        st.markdown("#### 🔬 2. Role-Specific Deep Dive (Filterable)")
        
        raw_sources = conn.execute("SELECT DISTINCT source FROM silver_all_jobs WHERE source IS NOT NULL AND source != '' ORDER BY source").df()['source'].tolist()
        db_levels = conn.execute("SELECT DISTINCT job_level FROM silver_all_jobs WHERE job_level != 'Error' AND job_level IS NOT NULL AND job_level != ''").df()['job_level'].tolist()

        opt_sources = tuple(["All Sources"] + raw_sources)

        hr_order = ["Intern", "Fresher", "Junior", "Middle", "Senior", "Manager", "Director"]
        valid_db_levels = [lvl for lvl in hr_order if lvl in db_levels]
        extra_levels = [lvl for lvl in db_levels if lvl not in hr_order]
        
        opt_levels = ["All Levels"] + valid_db_levels + extra_levels

        def enforce_level_selection():
            if st.session_state.filter_level_pill is None:
                st.session_state.filter_level_pill = "All Levels"

        if "filter_level_pill" not in st.session_state:
            st.session_state.filter_level_pill = "All Levels"

        st.markdown("##### 🔍 Filter Data")
        
        selected_source = st.selectbox("🌐 Select Data Source:", options=opt_sources)
        
        selected_level = st.pills(
            "🎯 Select Job Level:", 
            options=opt_levels, 
            key="filter_level_pill",
            on_change=enforce_level_selection
        )

        if not selected_level:
            selected_level = "All Levels"

        filters = ["status = 'Active'"]
        if selected_source != "All Sources":
            filters.append(f"source = '{selected_source}'")
        if selected_level != "All Levels":
            filters.append(f"job_level = '{selected_level}'")
            
        global_filter_sql = ""
        if filters:
            global_filter_sql = "AND " + " AND ".join(filters)
            
        dyn_jobs = conn.execute(f"SELECT COUNT(*) FROM silver_all_jobs WHERE job_level != 'Error' {global_filter_sql}").fetchone()[0]
        dyn_yoe = conn.execute(f"SELECT ROUND(AVG(min_years_of_experience), 1) FROM silver_all_jobs WHERE min_years_of_experience IS NOT NULL {global_filter_sql}").fetchone()[0]
        dyn_yoe = dyn_yoe if dyn_yoe is not None else 0
        
        eng_demand = conn.execute(f"SELECT COUNT(*) FROM silver_all_jobs WHERE english_requirement != 'Not mentioned' AND english_requirement IS NOT NULL {global_filter_sql}").fetchone()[0]
        eng_pct = round((eng_demand / dyn_jobs) * 100, 1) if dyn_jobs > 0 else 0

        col_d1, col_d2, col_d3 = st.columns(3)
        col_d1.metric(f"Filtered Jobs", f"{dyn_jobs:,}")
        col_d2.metric("Avg. Experience", f"{dyn_yoe} Yrs")
        col_d3.metric("English Required", f"{eng_pct}%", help="Percentage of JDs requiring English (from Intermediate level up)")

        dyn_col1, dyn_col2 = st.columns([1.5, 1])
        
        with dyn_col1:
            sql_tech_stack = f"""
                WITH unnested_skills AS (
                    SELECT 
                        UNNEST(from_json(ai_core_tech_stack, '["VARCHAR"]')) AS skill
                    FROM silver_all_jobs
                    WHERE ai_core_tech_stack IS NOT NULL
                      AND ai_core_tech_stack != '[]'
                      AND job_level != 'Error'
                      {global_filter_sql}
                )
                SELECT skill, COUNT(*) as total_mentions
                FROM unnested_skills
                WHERE skill != '' AND skill IS NOT NULL
                GROUP BY skill
                ORDER BY total_mentions DESC, skill ASC 
                LIMIT 10
            """
                
            df_skills = conn.execute(sql_tech_stack).df()

            if not df_skills.empty:
                fig_bar = px.bar(
                    df_skills, x='total_mentions', y='skill', orientation='h',
                    title=f"Top 10 Tech Stack",
                    color='total_mentions', color_continuous_scale='Reds'
                )
                fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(t=40, b=0, l=0, r=0))
                # Fixed Streamlit warning here
                st.plotly_chart(fig_bar, width="stretch")
            else:
                st.info("No tech stack data available for this filter.")

        with dyn_col2:
            df_eng_levels = conn.execute(f"""
                SELECT english_requirement, COUNT(*) as count 
                FROM silver_all_jobs 
                WHERE english_requirement IS NOT NULL {global_filter_sql}
                GROUP BY english_requirement
                ORDER BY count DESC, english_requirement ASC 
            """).df()
            
            if not df_eng_levels.empty:
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
                # Fixed Streamlit warning here
                st.plotly_chart(fig_eng, width="stretch")

    except Exception as e:
        st.error(f"Dashboard query error: {e}")
    finally:
        conn.close()

# ==========================================
# TAB 2: AI CAREER COACH (ENTERPRISE RAG)
# ==========================================
with tab2:
    st.subheader("🎯 Upload your CV & Get AI Gap Analysis")
    st.markdown("Strict Verification Pipeline: **Education -> Experience -> Tech Stack**.")
    
    uploaded_cv = st.file_uploader("Upload CV (PDF format only)", type=["pdf"])
    
    if uploaded_cv is not None:
        cv_bytes = uploaded_cv.getvalue()
        cv_hash = hashlib.md5(cv_bytes).hexdigest()

        # Initialize session state cache for CV analysis
        if "last_cv_hash" not in st.session_state:
            st.session_state.last_cv_hash = None
            st.session_state.cached_report = None
            st.session_state.cached_matched_jobs = None
            st.session_state.cached_yoe = 0
            st.session_state.cached_query = ""

        is_new_cv = (st.session_state.last_cv_hash != cv_hash)

        col_btn, _ = st.columns([1, 3])
        with col_btn:
            analyze_btn = st.button("🚀 Analyze CV & Match Jobs", type="primary")

        # Run analysis when user clicks OR retrieve from cache if already analyzed
        should_run_analysis = analyze_btn or (not is_new_cv and st.session_state.cached_report is not None)

        if should_run_analysis:
            if not OPENAI_API_KEY:
                st.error("⚠️ Missing OPENAI_API_KEY. Please check your .env file!")
                st.stop()

            # Only call OpenAI & RAG pipeline if it's a new CV or user explicitly clicked Analyze
            if is_new_cv or (analyze_btn and st.session_state.cached_report is None):
                with st.status("AI is processing your CV...", expanded=True) as status:
                    try:
                        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0)
                        
                        # STEP 1: READ PDF FILE
                        status.update(label="1. Loading and parsing CV document...")
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                            tmp_file.write(cv_bytes)
                            tmp_path = tmp_file.name
                        
                        loader = PyPDFLoader(tmp_path)
                        cv_pages = loader.load_and_split()
                        cv_text = " ".join([p.page_content for p in cv_pages])
                        os.remove(tmp_path) 

                        # STEP 1.5: QUERY TRANSFORMATION
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
                        
                        candidate_yoe = 0
                        search_query = cv_text 
                        
                        try:
                            lines = profile_response.strip().split('\n')
                            for line in lines:
                                if line.startswith("YOE:"):
                                    candidate_yoe = int(re.findall(r'\d+', line)[0])
                                elif line.startswith("QUERY:"):
                                    search_query = line.replace("QUERY:", "").strip()
                        except Exception as e:
                            st.warning("Error extracting YOE. Defaulting to 0 years.")

                        # STEP 2: LOAD QDRANT VECTOR DATABASE (READ-ONLY)
                        status.update(label="3. Accessing Qdrant Vector Database...")
                        
                        @st.cache_resource
                        def get_qdrant_client():
                            return QdrantClient(path=QDRANT_PATH)
                        
                        client = get_qdrant_client()
                        
                        if not client.collection_exists(collection_name=COLLECTION_NAME):
                            status.update(label="Vector Database not found!", state="error")
                            st.warning("⚠️ The AI system is currently synchronizing market data. Please check back in a few minutes!")
                            st.stop()

                        vectorstore = QdrantVectorStore(
                            client=client, 
                            collection_name=COLLECTION_NAME, 
                            embedding=embeddings,
                            sparse_embedding=sparse_embeddings, 
                            retrieval_mode=RetrievalMode.HYBRID 
                        )
                        
                        # ----------------------------------------------------
                        # RERANKER (LAZY LOADED)
                        # ----------------------------------------------------
                        status.update(label="4. Deep Search & Reranking Top matches...")
                        qdrant_filter = Filter(
                            must=[
                                FieldCondition(
                                    key="metadata.yoe", 
                                    range=Range(lte=candidate_yoe + 1)
                                )
                            ]
                        )
                        
                        base_retriever = vectorstore.as_retriever(
                            search_kwargs={"k": 30, "filter": qdrant_filter} 
                        )
                        
                        # Lazy load heavy reranker model on demand
                        bge_reranker_model = get_reranker_model()
                        compressor = CrossEncoderReranker(model=bge_reranker_model, top_n=10)
                        
                        compression_retriever = ContextualCompressionRetriever(
                            base_compressor=compressor, 
                            base_retriever=base_retriever
                        )
                        
                        matched_jobs = compression_retriever.invoke(search_query)

                        # Filter out any inactive or expired jobs directly against DuckDB
                        try:
                            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
                            db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
                            if os.path.exists(db_path):
                                check_conn = duckdb.connect(db_path, read_only=True)
                                job_ids = [doc.metadata.get("job_id") for doc in matched_jobs if doc.metadata.get("job_id")]
                                if job_ids:
                                    placeholders = ", ".join(["?"] * len(job_ids))
                                    active_rows = check_conn.execute(
                                        f"SELECT job_id FROM silver_all_jobs WHERE job_id IN ({placeholders}) AND status = 'Active'",
                                        job_ids
                                    ).fetchall()
                                    active_ids = {r[0] for r in active_rows}
                                    matched_jobs = [doc for doc in matched_jobs if doc.metadata.get("job_id") in active_ids]
                                check_conn.close()
                        except Exception:
                            pass
                        
                        if not matched_jobs:
                            st.warning(f"Unfortunately, no active jobs found matching {candidate_yoe} years of experience.")
                            st.stop()
                            
                        # Deduplicate jobs by URL to prevent duplicate cards
                        seen_urls = set()
                        unique_matched_jobs = []
                        for doc in matched_jobs:
                            u = doc.metadata.get("job_url", "")
                            if u not in seen_urls:
                                seen_urls.add(u)
                                unique_matched_jobs.append(doc)

                        jobs_context = "\n\n".join([f"Job {i+1}: {doc.page_content}" for i, doc in enumerate(unique_matched_jobs)])

                        # STEP 4: GENERATE HR EVALUATION REPORT
                        status.update(label="5. HR Expert is drafting the Gap Analysis...")
                        llm_eval = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
                        prompt = f"""
                        You are a highly analytical and strict HR Director in the IT industry.
                        Below is the candidate's CV and the Top matching jobs (already pre-filtered by experience level).
                        Ensure there are no duplicated job recommendations.

                        ### CANDIDATE'S ORIGINAL CV:
                        {cv_text[:3500]} 

                        ### TOP MATCHING JOBS:
                        {jobs_context}

                        ### EVALUATION TASKS (STRICTLY FOLLOW THIS ORDER):
                        1. **Background Assessment (Education & Learning):** Briefly comment on the candidate's university, major, certificates, or self-learning path. Is this foundation solid for long-term growth?
                        2. **Experience Assessment (Work History):** How many years of experience does the candidate have? Are the projects deep enough or superficial? Compared to the required experience of the Jobs above, is the candidate underqualified?
                        3. **Tech Stack Assessment (Tools & Frameworks):** Point out exactly which technical skills the candidate is strong at, and which required skills from the Jobs are missing (The Gap).
                        4. **30-Day Strategy:** Provide the most practical advice for the candidate to bridge the Gap before applying to these companies.

                        Respond in English, use clear Markdown formatting, and maintain a direct, professional, and uncompromising tone (do not flatter the candidate).
                        """
                        
                        response = llm_eval.invoke(prompt)
                        status.update(label="Analysis Complete!", state="complete")

                        # Save to session cache
                        st.session_state.last_cv_hash = cv_hash
                        st.session_state.cached_report = response.content
                        st.session_state.cached_matched_jobs = unique_matched_jobs
                        st.session_state.cached_yoe = candidate_yoe
                        st.session_state.cached_query = search_query

                    except Exception as e:
                        status.update(label="An error occurred!", state="error")
                        st.error(f"System Error: {e}")

            # RENDER RESULTS FROM SESSION CACHE
            if st.session_state.cached_report and st.session_state.cached_matched_jobs:
                st.write(f"*(AI estimated experience: **{st.session_state.cached_yoe} years**)*")
                st.write(f"*(Optimized Search Query: **{st.session_state.cached_query}**)*")

                col1, col2 = st.columns([1, 2])
                with col1:
                    st.info("📌 **Top Best Matching Jobs:**")
                    for job in st.session_state.cached_matched_jobs:
                        title = job.metadata.get("job_title", "View Job Details")
                        url = job.metadata.get("job_url", "#")
                        source = job.metadata.get("source", "Unknown")
                        
                        st.markdown(f"**[{title}]({url})** `[{source}]`")
                        clean_content = job.page_content.replace(f"Source: {source} | Title: {title} | ", "")
                        st.caption(clean_content)
                        st.divider()
                        
                with col2:
                    st.markdown(st.session_state.cached_report)