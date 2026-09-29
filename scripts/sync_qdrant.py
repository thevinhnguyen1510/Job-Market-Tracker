import os
import duckdb
import hashlib
import uuid
import sys
from dotenv import load_dotenv

# --- IMPORT REQUIRED MODELS FOR COLLECTION CREATION ---
from qdrant_client.models import VectorParams, Distance, SparseVectorParams

# --- LANGCHAIN & QDRANT CORE ---
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore, FastEmbedSparse, RetrievalMode

# 1. SETUP CONFIGURATION
load_dotenv()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
QDRANT_PATH = os.path.join(BASE_DIR, 'local_qdrant_db')
COLLECTION_NAME = "all_it_jobs_v6" 

print("INITIATING QDRANT VECTOR DB AUTO-SYNC (SOFT DELETE ENABLED)...")

try:
    client = QdrantClient(path=QDRANT_PATH)
except Exception as e:
    if "already accessed by another instance" in str(e).lower() or "locked" in str(e).lower():
        print("\n[!] LOCK CONFLICT: The local Qdrant database is currently locked by another process (e.g., Streamlit).")
        print("    -> Please temporarily stop Streamlit (Ctrl+C in its terminal), re-run this script, then restart Streamlit.\n")
        sys.exit(1)
    raise e

embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
sparse_embeddings = FastEmbedSparse(model_name="Qdrant/bm25")

# ========================================================
# AUTO-INITIALIZE HYBRID VECTOR SPACE IF NOT EXISTS
# ========================================================
is_first_run = False
if not client.collection_exists(collection_name=COLLECTION_NAME):
    print("First run detected: Auto-initializing Hybrid Search structure...")
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
        sparse_vectors_config={
            "langchain-sparse": SparseVectorParams()
        }
    )
    is_first_run = True # Flag to trigger a Full Load

vectorstore = QdrantVectorStore(
    client=client,
    collection_name=COLLECTION_NAME,
    embedding=embeddings,
    sparse_embedding=sparse_embeddings,
    retrieval_mode=RetrievalMode.HYBRID
)

# 2. CONNECT TO DUCKDB (Read-Only Mode)
db_path = os.path.join(BASE_DIR, 'job_market.duckdb')
conn = duckdb.connect(db_path, read_only=True)

# ========================================================
# TASK A: SOFT DELETE FOR INACTIVE / EXPIRED JOBS
# ========================================================
inactive_jobs_df = conn.execute("""
    SELECT job_id FROM silver_all_jobs
    WHERE status != 'Active' OR status IS NULL
""").df()

if not inactive_jobs_df.empty:
    print(f"Found {len(inactive_jobs_df)} inactive/expired jobs in DB. Applying Soft Delete (payload: is_active=False)...")
    inactive_ids = [str(uuid.UUID(hashlib.md5(str(j_id).encode()).hexdigest())) for j_id in inactive_jobs_df['job_id']]
    BATCH_SIZE = 500
    deactivated_count = 0
    for i in range(0, len(inactive_ids), BATCH_SIZE):
        batch = inactive_ids[i:i + BATCH_SIZE]
        existing = client.retrieve(collection_name=COLLECTION_NAME, ids=batch)
        existing_ids = [p.id for p in existing]
        if existing_ids:
            client.set_payload(
                collection_name=COLLECTION_NAME,
                payload={
                    "is_active": False,
                    "status": "Inactive"
                },
                points=existing_ids
            )
            deactivated_count += len(existing_ids)
    print(f"   [OK] Soft Delete in Qdrant completed ({deactivated_count} existing points set to is_active=False)!")

# ========================================================
# TASK B: DATA UPSERT (Auto-detect Full vs Incremental Load)
# ========================================================
if is_first_run:
    # First run: Fetch ALL Active jobs
    print("Executing Initial Full Load...")
    sql_query = "SELECT * FROM silver_all_jobs WHERE status = 'Active'"
else:
    # Daily run: Fetch ONLY NEW jobs from today
    print("Executing Daily Incremental Upsert...")
    sql_query = "SELECT * FROM silver_all_jobs WHERE status = 'Active' AND DATE(processed_at) = CURRENT_DATE"

jobs_df = conn.execute(sql_query).df()

if jobs_df.empty:
    print("No new active jobs to vectorize today.")
else:
    print(f"Found {len(jobs_df)} jobs. Calling OpenAI for Embeddings...")
    job_docs = []
    doc_ids = []
    
    for _, row in jobs_df.iterrows():
        content = f"Source: {row['source']} | Title: {row['job_title']} | Tech: {row['ai_core_tech_stack']} | Level: {row['job_level']} | Exp: {row['min_years_of_experience']} years | English: {row['english_requirement']}"
        job_docs.append(Document(
            page_content=content,
            metadata={
                "job_id": row['job_id'],      
                "job_title": row['job_title'], 
                "job_url": row['job_url'], 
                "company_name": str(row['company_name']) if row.get('company_name') else "Tech Company",
                "ai_job_role": row['ai_job_role'],
                "ai_core_tech_stack": row['ai_core_tech_stack'],
                "job_level": row['job_level'],
                "english_requirement": row['english_requirement'],
                "salary_raw": str(row['salary_raw']) if row.get('salary_raw') else "Negotiable",
                "yoe": row['min_years_of_experience'],
                "source": row['source'],
                "status": row['status'],
                "is_active": True,
                "first_seen_at": str(row['first_seen_at']) if row.get('first_seen_at') is not None else '',
                "days_open": int(row['days_open']) if row.get('days_open') is not None else 0
            }
        ))
        
        hash_id = str(uuid.UUID(hashlib.md5(str(row['job_id']).encode()).hexdigest()))
        doc_ids.append(hash_id)

    # add_documents acts as an UPSERT in Qdrant based on the provided IDs
    vectorstore.add_documents(documents=job_docs, ids=doc_ids)

    # Ensure top-level payload also has is_active = True for fast filtering
    BATCH_SIZE = 500
    for i in range(0, len(doc_ids), BATCH_SIZE):
        batch = doc_ids[i:i + BATCH_SIZE]
        client.set_payload(
            collection_name=COLLECTION_NAME,
            payload={"is_active": True, "status": "Active"},
            points=batch
        )

    print(f"   [OK] Successfully synced {len(jobs_df)} Vectors into the Hybrid space!")

# ========================================================
# TASK C: ENSURE EXISTING ACTIVE POINTS HAVE is_active = True
# ========================================================
active_jobs_df = conn.execute("SELECT job_id FROM silver_all_jobs WHERE status = 'Active'").df()
if not active_jobs_df.empty:
    active_ids = [str(uuid.UUID(hashlib.md5(str(j_id).encode()).hexdigest())) for j_id in active_jobs_df['job_id']]
    BATCH_SIZE = 500
    for i in range(0, len(active_ids), BATCH_SIZE):
        batch = active_ids[i:i + BATCH_SIZE]
        existing = client.retrieve(collection_name=COLLECTION_NAME, ids=batch)
        existing_ids = [p.id for p in existing]
        if existing_ids:
            client.set_payload(
                collection_name=COLLECTION_NAME,
                payload={"is_active": True, "status": "Active"},
                points=existing_ids
            )

conn.close()
client.close()
print("\nVECTOR DB SYNC COMPLETED.")