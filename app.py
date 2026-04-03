import streamlit as st
import duckdb
import pandas as pd
import os
import tempfile
import re
from dotenv import load_dotenv
from langchain_community.document_loaders import PyPDFLoader
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document

# 1. SETUP & CONFIGURATION
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

st.set_page_config(page_title="IT Job Market & AI Coach", layout="wide", page_icon="🚀")
st.title("🚀 IT Job Market Tracker & AI Career Coach")

tab1, tab2 = st.tabs(["📊 Market Dashboard", "🤖 AI Career Coach (RAG)"])

# ==========================================
# TAB 1: DASHBOARD
# ==========================================
with tab1:
    st.markdown("Data is automatically extracted and standardized by **LLM (GPT-4o-mini)**.")
    conn = duckdb.connect('job_market.duckdb', read_only=True)
    try:
        st.subheader("🔥 Top In-Demand Tech Skills")
        df_skills = conn.execute("""
            WITH cleaned_strings AS (
                SELECT job_url, REPLACE(REPLACE(REPLACE(ai_core_tech_stack, '[', ''), ']', ''), '"', '') AS clean_stack
                FROM silver_itviec_jobs WHERE ai_core_tech_stack IS NOT NULL
            ),
            unnested_skills AS (
                SELECT job_url, TRIM(UNNEST(string_split(clean_stack, ','))) AS skill
                FROM cleaned_strings WHERE clean_stack != ''
            )
            SELECT skill, COUNT(DISTINCT job_url) AS total_mentions
            FROM unnested_skills WHERE skill != '' GROUP BY skill ORDER BY total_mentions DESC LIMIT 15
        """).df()

        if not df_skills.empty:
            st.bar_chart(data=df_skills.set_index('skill'), y='total_mentions', color="#ff4b4b")
        else:
            st.warning("No data found in the Gold Layer.")
    except Exception as e:
        st.error(f"Database query error: {e}")
    finally:
        conn.close()

# ==========================================
# TAB 2: AI CAREER COACH
# ==========================================
with tab2:
    st.subheader("Upload your CV & Get AI Gap Analysis")
    st.markdown("System strictly checks: **Education -> Experience -> Tech Stack**.")
    
    uploaded_cv = st.file_uploader("Upload CV (PDF format only)", type=["pdf"])
    
    if uploaded_cv is not None:
        if st.button("Analyze CV & Find Jobs", type="primary"):
            
            if not OPENAI_API_KEY:
                st.error("⚠️ Missing OPENAI_API_KEY. Please check the .env file!")
                st.stop()

            with st.status("AI is processing...", expanded=True) as status:
                try:
                    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0)

                    # BƯỚC 1: READ PDF FILE
                    status.update(label="1. Reading and extracting CV...")
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                        tmp_file.write(uploaded_cv.getvalue())
                        tmp_path = tmp_file.name
                    
                    loader = PyPDFLoader(tmp_path)
                    cv_pages = loader.load_and_split()
                    cv_text = " ".join([p.page_content for p in cv_pages])
                    os.remove(tmp_path) 

                    # BƯỚC 1.5: Analyze work experience to filter jobs (PRE-FILTERING)
                    status.update(label="2. Analyzing work experience...")
                    yoe_prompt = f"""
                    Read the CV below and return EXACTLY 1 integer representing the total years of professional work experience of the candidate.
                    - Do NOT count university study time.
                    - Include internship periods.
                    - Return 0 if no work experience.
                    CV Text: {cv_text[:2000]}
                    """
                    yoe_response = llm.invoke(yoe_prompt).content
                    numbers = re.findall(r'\d+', yoe_response)
                    candidate_yoe = int(numbers[0]) if numbers else 0
                    st.write(f"*(AI estimates you have about {candidate_yoe} years of experience)*")

                    # BƯỚC 2: GET JOBS FROM DUCKDB WITH JOB URL
                    status.update(label="3. Filtering jobs by experience...")
                    conn = duckdb.connect('job_market.duckdb', read_only=True)
                    
                    # IMPORTANT: Select job_url field
                    query = f"""
                        SELECT job_url, job_title, ai_core_tech_stack, min_years_of_experience, english_requirement, job_level 
                        FROM silver_itviec_jobs 
                        WHERE job_level != 'Error' 
                          AND min_years_of_experience <= {candidate_yoe + 1}
                        LIMIT 200
                    """
                    jobs_df = conn.execute(query).df()
                    conn.close()

                    if jobs_df.empty:
                        st.warning(f"No jobs found matching your experience level of {candidate_yoe} years.")
                        st.stop()

                    # BƯỚC 3: SEARCH VECTOR BY TECH STACK (ATTACH URL TO METADATA)
                    status.update(label="4. Matching tech stack...")
                    job_docs = []
                    for _, row in jobs_df.iterrows():
                        content = f"Title: {row['job_title']} | Tech: {row['ai_core_tech_stack']} | Level: {row['job_level']} | Exp: {row['min_years_of_experience']} years | English: {row['english_requirement']}"
                        
                        # Attach title and url to Document metadata
                        job_docs.append(Document(
                            page_content=content,
                            metadata={"job_title": row['job_title'], "job_url": row['job_url']}
                        ))

                    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
                    vectorstore = FAISS.from_documents(job_docs, embeddings) 

                    with st.expander("🛠️ See what's inside the FAISS Vector DB"):
                        # FAISS hides the original data in an object called docstore
                        all_docs_in_faiss = vectorstore.docstore._dict.values()
                        st.write(f"Total jobs vectorized and stored in FAISS: **{len(all_docs_in_faiss)}**")
                        
                        # Convert to table format for easier viewing
                        debug_data = []
                        for doc in all_docs_in_faiss:
                            debug_data.append({
                                "Job Title": doc.metadata.get("job_title"),
                                "Text Content (for Vector comparison)": doc.page_content,
                                "URL Link": doc.metadata.get("job_url")
                            })
                        st.dataframe(debug_data)
                    
                    retriever = vectorstore.as_retriever(search_kwargs={"k": 3})
                    matched_jobs = retriever.invoke(cv_text)
                    jobs_context = "\n\n".join([f"Job {i+1}: {doc.page_content}" for i, doc in enumerate(matched_jobs)])

                    # BƯỚC 4: CREATE COMPREHENSIVE EVALUATION REPORT
                    status.update(label="5. HR Expert is writing comprehensive evaluation...")
                    llm_eval = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
                    prompt = f"""
                    You are a very strict and logical HR Director in the IT industry.
                    Below is the candidate's CV and the top 3 matching jobs (pre-filtered by our system to ensure experience requirements are not too demanding).

                    ### CV OF CANDIDATE:
                    {cv_text[:3500]} 

                    ### TOP 3 JOBS MATCH:
                    {jobs_context}

                    ### MISSION EVALUATION (FOLLOW THE ORDER BELOW):
                    1. **Evaluate Education & Learning Path:** Quickly comment on the candidate's school, major, certifications, or self-learning process. Does this foundation suit long-term growth?
                    2. **Evaluate Experience (Work Duration):** How many years of experience does the candidate have? Are the projects they've worked on deep enough, or just surface-level? Compared to the experience requirements of the 3 Jobs above, does the candidate fall behind?
                    3. **Evaluate Tech Stack (Tools):** Identify exactly which technical skills the candidate is strong in, and which skills the Job requires but the CV is missing (Gap).
                    4. **30-Day Strategy:** The most practical advice to help the candidate fill the Gap before applying to these 3 companies.

                    Answer in English, use clear Markdown, direct tone, no flattery.
                    """
                    
                    response = llm_eval.invoke(prompt)
                    status.update(label="✅ Analysis complete!", state="complete")
                    
                    # Display Results (Include Job Links)
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.info("📌 **Top 3 best matching Jobs:**")
                        for job in matched_jobs:
                            title = job.metadata.get("job_title", "View job details")
                            url = job.metadata.get("job_url", "#")
                            
                            # Display Job Title as a clickable Link
                            st.markdown(f"**[{title}]({url})**")
                            # Display technology summary below
                            clean_content = job.page_content.replace(f"Title: {title} | ", "")
                            st.caption(clean_content)
                            st.divider() # Divider line between Jobs
                            
                    with col2:
                        st.markdown(response.content)

                except Exception as e:
                    status.update(label="❌ Error occurred!", state="error")
                    st.error(f"System error: {e}")