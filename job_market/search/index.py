"""Shared Qdrant collection and embedding configuration for sync and retrieval."""

from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import FastEmbedSparse, QdrantVectorStore, RetrievalMode
from qdrant_client import QdrantClient, models

from job_market.config import Settings

DENSE_MODEL = "text-embedding-3-small"
DENSE_SIZE = 1536
SPARSE_MODEL = "Qdrant/bm25"
SPARSE_VECTOR_NAME = "langchain-sparse"


def connect(settings: Settings) -> QdrantClient:
    """Open the embedded Qdrant store at the configured path."""
    return QdrantClient(path=str(settings.qdrant_path))


def ensure_collection(client: QdrantClient, collection: str) -> bool:
    """Create the hybrid collection if missing; return True if it was created."""
    if client.collection_exists(collection):
        return False
    client.create_collection(
        collection,
        vectors_config=models.VectorParams(size=DENSE_SIZE, distance=models.Distance.COSINE),
        sparse_vectors_config={SPARSE_VECTOR_NAME: models.SparseVectorParams()},
    )
    return True


def hybrid_store(client: QdrantClient, collection: str) -> QdrantVectorStore:
    """Build the vector store with the dense and sparse models used for search."""
    return QdrantVectorStore(
        client=client,
        collection_name=collection,
        embedding=OpenAIEmbeddings(model=DENSE_MODEL),
        sparse_embedding=FastEmbedSparse(model_name=SPARSE_MODEL),
        sparse_vector_name=SPARSE_VECTOR_NAME,
        retrieval_mode=RetrievalMode.HYBRID,
    )
