"""Build a query, retrieve jobs and rerank them for a CV profile."""

import logging

from langchain_core.documents import Document
from pydantic import BaseModel, Field
from qdrant_client import models

from job_market.search.activity import ACTIVE_FIELD

logger = logging.getLogger(__name__)
RETRIEVAL_LIMIT = 30
RERANK_LIMIT = 12
FALLBACK_LIMIT = 6
YOE_TOLERANCE = 2


class CandidateProfile(BaseModel):
    """Fields from a CV used to query and filter job postings."""

    yoe: int = Field(default=0, ge=0)
    target_role: str = "Unknown"
    primary_languages: list[str] = Field(default_factory=list)
    core_skills: list[str] = Field(default_factory=list)
    search_summary: str = ""


def search_filter(profile: CandidateProfile) -> models.Filter:
    """Keep active jobs within the experience tolerance or with null experience."""
    return models.Filter(must=[
        models.FieldCondition(key=ACTIVE_FIELD, match=models.MatchValue(value=True)),
        models.Filter(should=[
            models.FieldCondition(key="metadata.yoe", range=models.Range(lte=profile.yoe + YOE_TOLERANCE)),
            models.IsNullCondition(is_null=models.PayloadField(key="metadata.yoe")),
        ]),
    ])


def retrieve_jobs(store, profile: CandidateProfile) -> list[Document]:
    """Build a query from the candidate profile and retrieve filtered hybrid matches."""
    role = profile.target_role
    if "backend" in role.lower() or "software engineer" in role.lower():
        role = "Backend / Software Engineer"
    query = " ".join([
        role, ", ".join(profile.primary_languages), ", ".join(profile.core_skills), profile.search_summary
    ])
    return store.similarity_search(query=query, k=RETRIEVAL_LIMIT, filter=search_filter(profile))


def rank_jobs(candidates: list[Document], profile: CandidateProfile, model) -> list[Document]:
    """Rerank candidates and deduplicate by URL, falling back to retrieval order."""
    from langchain_classic.retrievers.document_compressors import CrossEncoderReranker

    try:
        reranker = CrossEncoderReranker(model=model, top_n=RERANK_LIMIT)
        ranked = reranker.compress_documents(candidates, profile.search_summary)
    except Exception as exc:
        logger.warning("Reranker failed; using retrieval order (%s)", type(exc).__name__)
        ranked = []
    unique = {}
    for document in ranked or candidates[:FALLBACK_LIMIT]:
        unique.setdefault(document.metadata.get("job_url", ""), document)
    return list(unique.values())
