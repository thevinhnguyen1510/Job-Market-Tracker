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

                        # STEP 1.5: STRUCTURED PROFILE EXTRACTION (ROLE + TECH STACK + YOE)
                        status.update(label="2. Analyzing candidate profile & Tech Stack...")
                        import json

                        search_profile_prompt = f"""
                        You are an expert IT Technical Recruiter.
                        Analyze the following CV and extract the candidate's core profile.
                        Return a STRICT JSON object (no markdown, no backticks, only valid raw JSON):
                        {{
                            "yoe": <integer, practical years of work experience excluding university/internship study>,
                            "target_role": "<primary job role, choose closest from: 'Backend', 'Frontend', 'Fullstack', 'Mobile', 'Data Engineer', 'Data Scientist', 'Data/Business Analyst', 'AI/Machine Learning', 'DevOps/Cloud', 'QA/QC/Tester'>",
                            "primary_languages": [<list of 1-3 primary programming languages/technologies the candidate specializes in, e.g. ["C#", ".NET"] or ["Java"] or ["Python"] or ["JavaScript/TypeScript"]>],
                            "core_skills": [<list of main frameworks/tools/databases, e.g. ["ASP.NET Core", "SQL Server", "Entity Framework", "Redis", "RESTful API"]>],
                            "search_summary": "<A 1-sentence dense summary strictly focusing on role and tech stack, e.g. 'Junior Backend Developer specializing in C#, .NET, ASP.NET Core, SQL Server, Redis, RESTful API'>"
                        }}

                        CV Text:
                        {cv_text[:6000]}
                        """

                        candidate_profile = {
                            "yoe": 0,
                            "target_role": "Unknown",
                            "primary_languages": [],
                            "core_skills": [],
                            "search_summary": cv_text[:500]
                        }

                        try:
                            profile_response = llm.invoke(search_profile_prompt).content
                            clean_json = re.sub(r'^```json\s*|\s*```$', '', profile_response.strip(), flags=re.MULTILINE).strip()
                            parsed = json.loads(clean_json)
                            candidate_profile.update(parsed)
                        except Exception:
                            pass

                        candidate_yoe = int(candidate_profile.get("yoe", 0))
                        target_role = candidate_profile.get("target_role", "Backend")
                        primary_langs = [p.strip() for p in candidate_profile.get("primary_languages", []) if p.strip()]
                        core_skills = [c.strip() for c in candidate_profile.get("core_skills", []) if c.strip()]
                        search_query = candidate_profile.get("search_summary", cv_text[:500])

                        # STEP 2: MULTI-STAGE RETRIEVAL & SMART GATEKEEPER
                        status.update(label="3. Filtering jobs by Target Role & Primary Tech Stack...")

                        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
                        db_path = os.path.join(BASE_DIR, 'job_market.duckdb')

                        conn_match = duckdb.connect(db_path, read_only=True)
                        all_active_df = conn_match.execute("""
                            SELECT job_id, job_url, job_title, ai_job_role, ai_core_tech_stack, min_years_of_experience, job_level, source, english_requirement
                            FROM silver_all_jobs
                            WHERE status = 'Active'
                        """).df()
                        conn_match.close()

                        # Incompatible roles to prevent cross-domain mismatch (e.g. Backend vs Data Analyst)
                        incompatible_roles = {
                            "Backend": ["Data/Business Analyst", "Data Scientist", "UI/UX Designer", "Product Owner/Manager", "QA/QC/Tester"],
                            "Frontend": ["Data/Business Analyst", "Data Scientist", "AI/Machine Learning", "DevOps/Cloud"],
                            "Fullstack": ["Data/Business Analyst", "Data Scientist", "UI/UX Designer", "Product Owner/Manager"],
                            "Data Engineer": ["Frontend", "UI/UX Designer", "Mobile"],
                            "Data/Business Analyst": ["Mobile", "Frontend", "DevOps/Cloud"],
                            "AI/Machine Learning": ["Frontend", "UI/UX Designer", "Mobile"],
                            "DevOps/Cloud": ["Frontend", "UI/UX Designer", "Data/Business Analyst"],
                            "Mobile": ["Data/Business Analyst", "Data Scientist", "AI/Machine Learning", "DevOps/Cloud"],
                            "QA/QC/Tester": ["Data Scientist", "AI/Machine Learning"]
                        }

                        banned_roles = incompatible_roles.get(target_role, [])
                        primary_langs_lower = [p.lower() for p in primary_langs]
                        core_skills_lower = [c.lower() for c in core_skills]

                        candidates_pool = []

                        for _, row in all_active_df.iterrows():
                            role = str(row['ai_job_role'])
                            title = str(row['job_title'])
                            tech_str = str(row['ai_core_tech_stack']).lower()
                            yoe = row['min_years_of_experience']

                            # 1. GATEKEEPER: Chặn triệt để khác Role
                            if role in banned_roles:
                                continue

                            # 2. TECH STACK MATCHING:
                            matched_primary = any(p in tech_str or p in title.lower() for p in primary_langs_lower)

                            score = 0
                            if matched_primary:
                                score += 150 # Ưu tiên tuyệt đối job đúng tech stack chính
                            else:
                                # Nếu ứng viên có stack chính rõ ràng (như C#/.NET), không nhồi job Java/Python/Golang vào
                                competing_major_langs = ["java", "spring boot", "python", "golang", "go", "php", "ruby", "rust"]
                                has_competing_only = any(lang in tech_str or lang in title.lower() for lang in competing_major_langs)
                                if has_competing_only and primary_langs_lower:
                                    continue
                                score += 10

                            # Match kỹ năng phụ / công cụ / DB
                            matched_skills_count = sum(1 for s in core_skills_lower if s in tech_str or s in title.lower())
                            score += matched_skills_count * 10

                            # Thưởng điểm cho Role
                            if target_role.lower() in role.lower() or target_role.lower() in title.lower():
                                score += 30
                            elif "developer" in title.lower() or "engineer" in title.lower() or "software" in title.lower():
                                score += 10

                            # YOE Soft Scoring (Không loại cứng, ưu tiên cấp bậc hợp lý)
                            if yoe is not None:
                                diff = yoe - candidate_yoe
                                if diff <= 0:
                                    score += 30 # Entry / Junior
                                elif diff <= 2:
                                    score += 20 # 1-2 năm chênh lệch (vừa sức ứng tuyển)
                                elif diff <= 4:
                                    score += 5  # Middle / Senior
                                else:
                                    score -= 30 # Yêu cầu quá cao
                            else:
                                score += 15

                            doc = Document(
                                page_content=f"Source: {row['source']} | Title: {row['job_title']} | Tech: {row['ai_core_tech_stack']} | Level: {row['job_level']} | Exp: {row['min_years_of_experience']} years | English: {row['english_requirement']}",
                                metadata={
                                    "job_id": row['job_id'],
                                    "job_title": row['job_title'],
                                    "job_url": row['job_url'],
                                    "yoe": row['min_years_of_experience'],
                                    "source": row['source'],
                                    "status": "Active",
                                    "score": score
                                }
                            )
                            candidates_pool.append((score, doc))

                        candidates_pool.sort(key=lambda x: x[0], reverse=True)
                        top_candidates = [doc for score, doc in candidates_pool[:20]]

                        if not top_candidates:
                            st.warning(f"⚠️ Hiện tại chưa tìm thấy công việc **{target_role}** phù hợp với Tech Stack (**{', '.join(primary_langs)}**) trong cơ sở dữ liệu hiện tại.")
                            st.info("💡 **Gợi ý:** Cơ sở dữ liệu hiện tại tập trung nhiều vào Data Engineer, Data Analyst, AI/ML. Bạn có thể mở rộng danh sách từ khóa cào dữ liệu cho pipeline để thu thập thêm các job Backend / .NET!")
                            st.stop()

                        # STEP 3: RERANKER (DEEP RE-RANKING ON FILTERED CANDIDATES)
                        status.update(label="4. Deep Search & Reranking Top matches...")
                        bge_reranker_model = get_reranker_model()
                        compressor = CrossEncoderReranker(model=bge_reranker_model, top_n=6)
                        try:
                            matched_jobs = compressor.compress_documents(top_candidates, search_query)
                        except Exception:
                            matched_jobs = top_candidates[:6]

                        if not matched_jobs:
                            matched_jobs = top_candidates[:6]

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
                        {cv_text} 

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
                        st.session_state.cached_role = target_role
                        st.session_state.cached_langs = primary_langs

                    except Exception as e:
                        status.update(label="An error occurred!", state="error")
                        st.error(f"System Error: {e}")

            # RENDER RESULTS FROM SESSION CACHE
            if st.session_state.cached_report and st.session_state.cached_matched_jobs:
                detected_role = st.session_state.get("cached_role", "Software Engineer")
                detected_langs = st.session_state.get("cached_langs", [])
                langs_display = f" | Target Stack: **{', '.join(detected_langs)}**" if detected_langs else ""
                st.markdown(f"🎯 **AI Candidate Profile:** Role: **{detected_role}**{langs_display} | Experience: **{st.session_state.cached_yoe} yrs**")
                st.caption(f"🔍 **Optimized Query:** `{st.session_state.cached_query}`")

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