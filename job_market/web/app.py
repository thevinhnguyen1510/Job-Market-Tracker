"""Streamlit application entry point."""

import streamlit as st

from job_market.config import load_settings
from job_market.web import career_coach, dashboard


def main() -> None:
    """Load settings and render the dashboard and CV analysis tabs."""
    settings = load_settings()
    st.set_page_config(page_title="IT Job Market & AI Coach", layout="wide", page_icon="🚀")
    st.title("🚀 IT Job Market Tracker & AI Career Coach")
    market_tab, coach_tab = st.tabs(["📊 Market Dashboard", "🤖 AI Career Coach (RAG)"])
    with market_tab:
        dashboard.render(settings)
    with coach_tab:
        career_coach.render(settings)
