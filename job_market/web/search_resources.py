"""Keep expensive retrieval resources alive across Streamlit reruns."""

import streamlit as st
from langchain_community.cross_encoders import HuggingFaceCrossEncoder

from job_market.config import Settings
from job_market.search.index import connect, hybrid_store


@st.cache_resource(show_spinner=False)
def get_qdrant_vectorstore(settings: Settings):
    """Open and cache the configured hybrid store across Streamlit reruns."""
    client = connect(settings)
    try:
        return hybrid_store(client, settings.qdrant_collection)
    except Exception:
        client.close()
        raise


@st.cache_resource(show_spinner="Loading relevance reranker...")
def get_reranker_model():
    """Load and cache the cross-encoder used to rerank job candidates."""
    return HuggingFaceCrossEncoder(model_name="BAAI/bge-reranker-base")
