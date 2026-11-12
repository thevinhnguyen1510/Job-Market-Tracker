# Operating and debugging the pipeline

Run the CLI from the repository root with the project's Python environment. The command prints stage-level logs; use `LOG_LEVEL=DEBUG` for extra dependency diagnostics.

| Symptom / Airflow task | Run separately | Where to inspect |
| --- | --- | --- |
| No list batch: `crawl_itviec`, `crawl_topcv` | `python -m job_market crawl <source>` | Source parser and fetch loop in `job_market/ingestion/<source>/crawl.py`; inspect keyword/page counts and HTTP failures. |
| Missing descriptions: `enrich_itviec`, `enrich_topcv` | `python -m job_market enrich <source>` | Source adapter's `enrich.py` and the latest raw file in `data/landing/<source>/`; TopCV also needs FlareSolverr. |
| Bronze not updated: `ingest_landing_to_bronze` | `python -m job_market ingest` | `job_market/warehouse/ingest.py`; enriched inputs stay in landing until the SQL write succeeds. |
| AI fields missing: `ai_extract_itviec`, `ai_extract_topcv` | `python -m job_market extract <source>` | `job_market/enrichment/extract.py`; check pending and failed counts. |
| SQL build/test fails: `dbt_build_silver`, `dbt_build_gold`, `dbt_test_data_quality` | From `analytics_dbt/`: `dbt --profiles-dir . build` | The failing SQL model, its inputs and dbt logs. |
| Search results stale: `sync_qdrant_vector_db` | `python -m job_market sync-search` | `job_market/search/sync.py`; check collection, upsert count and the actual Qdrant path used by Streamlit. |
| CV analysis fails | `streamlit run app.py` | CV parsing/profile generation in `web/cv_analysis.py`, filtering and ranking in `search/matching.py`, UI in `web/career_coach.py`. |

## Verify old listings

`python -m job_market verify-expired` checks older active listings and flags confirmed expirations in Bronze. It writes to DuckDB, so run it outside a scheduled writer run. It does not invoke other stages. Rebuild and test dbt models before running `sync-search`; otherwise search still reads the previous Silver state.

## Upgrade from the flat scripts

Replace old `python scripts/...py` commands with the CLI in the table. Use `inspect warehouse` and `inspect search` for diagnostics; rebuild derived tables with dbt instead of using the old drop-table helpers. The DAG ID, task IDs, dbt model names, database filenames and Qdrant point IDs are unchanged. Recreate the Compose containers after updating the code mount; no data move is required.

## Known runtime limits

- Embedded DuckDB and Qdrant are local stores. Use the same configured path for a publisher and its reader and avoid concurrent writers. A Compose worker does not automatically share its embedded index with the host.
- Vector sync still selects AI extractions from the current calendar day on subsequent runs. This is not a persisted checkpoint; failed runs need reconciliation before trusting index completeness.
- Source collection is live. The archived batches can support controlled replay, but there is no date-range backfill workflow. Older batches must not be replayed into live Bronze without checking versions.
- Enrichment handles the latest raw batch per source per run. Check landing for older files when diagnosing a backlog.
