"""Render the CV workflow and keep successful results in the user session."""

import hashlib
import logging
import os
import re

import streamlit as st

from job_market.config import Settings
from job_market.search.matching import rank_jobs, retrieve_jobs
from job_market.web.cv_analysis import extract_profile, generate_report, read_cv
from job_market.web.search_resources import get_qdrant_vectorstore, get_reranker_model

logger = logging.getLogger(__name__)


def render(settings: Settings) -> None:
    """Run CV analysis on request and cache successful results for the session."""
    st.subheader("🎯 Upload your CV & Get AI Gap Analysis")
    uploaded_cv = st.file_uploader("Upload CV (PDF format only)", type=["pdf"])
    if uploaded_cv is None:
        return

    pdf = uploaded_cv.getvalue()
    cv_key = (hashlib.sha256(pdf).hexdigest(), settings.qdrant_path, settings.qdrant_collection)
    analyze = st.button("🚀 Analyze CV & Match Jobs", type="primary")
    cached = st.session_state.get("career_coach_result")

    if analyze:
        if not os.getenv("OPENAI_API_KEY"):
            st.error("OPENAI_API_KEY is required for CV analysis.")
            return
        with st.status("Analyzing your CV...", expanded=True) as status:
            try:
                cv_text = read_cv(pdf)
                status.update(label="Extracting candidate profile...")
                profile = extract_profile(cv_text)
                status.update(label="Retrieving job candidates...")
                candidates = retrieve_jobs(get_qdrant_vectorstore(settings), profile)
                if not candidates:
                    status.update(label="No matching jobs", state="complete")
                    st.warning("No active jobs matched this profile.")
                    return
                status.update(label="Reranking relevant jobs...")
                jobs = rank_jobs(candidates, profile, get_reranker_model())
                status.update(label="Writing the gap analysis...")
                report = generate_report(cv_text, jobs)
                cached = (cv_key, profile, jobs, report)
                st.session_state["career_coach_result"] = cached
                status.update(label="Analysis complete", state="complete")
            except Exception as exc:
                logger.warning("CV analysis failed: %s", type(exc).__name__)
                status.update(label="CV analysis failed", state="error")
                st.error(str(exc) if isinstance(exc, ValueError) else "Analysis failed. Check the application logs.")
                return

    if cached is not None and cached[0] == cv_key:
        render_result(*cached[1:])


def render_result(profile, jobs, report: str) -> None:
    """Display the candidate profile, ranked job cards, and advice."""
    languages = ", ".join(profile.primary_languages)
    st.markdown(f"**Candidate profile:** {profile.target_role} | {profile.yoe} yrs | {languages}")
    st.caption(f"Search summary: {profile.search_summary}")
    matches, advice = st.columns([1, 2])
    with matches:
        st.info("Top matching jobs")
        for job in jobs:
            raw_title = str(job.metadata.get("job_title", "View job details"))
            title = re.sub(r"^(Tin mới Nổi bật|Nổi bật|Tin mới)\s*", "", raw_title, flags=re.IGNORECASE)
            title = " ".join(title.split())
            url = job.metadata.get("job_url", "#")
            source = job.metadata.get("source", "Unknown")
            company = job.metadata.get("company_name", "Tech Company")
            days_open = job.metadata.get("days_open", 0)
            salary = job.metadata.get("salary_raw") or "Negotiable"
            freshness = "Today" if days_open == 0 else f"{days_open}d since first seen"
            st.markdown(f"**[{title}]({url})**")
            st.caption(f"{company} · {source} · {freshness} · {salary}")
            st.caption(job.page_content.removeprefix(f"Source: {source} | Title: {raw_title} | "))
            st.divider()
    with advice:
        st.markdown(report)
