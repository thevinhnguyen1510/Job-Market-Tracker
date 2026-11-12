# IT Job Market Tracker

A daily batch pipeline for Vietnamese IT job listings. It collects jobs from ITviec and TopCV, extracts structured fields from job descriptions, builds DuckDB analytics with dbt, and publishes a Qdrant index for CV matching.

```text
ITviec / TopCV → raw Parquet → enriched Parquet → DuckDB Bronze
  → AI extraction → dbt Silver and Gold → dbt tests → Qdrant search index
Streamlit reads DuckDB for analytics and Qdrant for CV matching.
```

## Where to look

```text
app.py                     Streamlit launcher
dags/                      Schedule and task dependencies; no business logic
job_market/
  __main__.py              CLI entry point used by Airflow and local operators
  config.py                Runtime settings and storage paths
  ingestion/
    itviec/                ITviec listing and detail adapters
    topcv/                 TopCV listing and detail adapters
    landing.py             Shared Parquet batch handling
  warehouse/               Parquet-to-Bronze loading
  enrichment/              AI extraction and its output schema
  search/                  Qdrant setup, publishing, filters and ranking
  web/                     Dashboard, CV analysis and Streamlit presentation
  operations/              Inspection and explicit manual maintenance
analytics_dbt/             SQL models, macros, seeds and data tests
tests/                     Offline Python tests
data/                      Runtime landing and archive data
docs/                      Architecture and operational notes
```

## Run locally

Use Python 3.10+ and run from the repository root. Install `requirements.txt` in a virtual environment. If you do not already have a `.env`, copy [`.env.example`](.env.example) and set the credentials required by the stages you intend to run. Keep actual credentials out of source control. TopCV collection requires Chrome and Xvfb; the Airflow image supplies them.

```sh
python -m job_market --help
python -m job_market crawl itviec
python -m job_market enrich itviec
python -m job_market crawl topcv
python -m job_market enrich topcv
python -m job_market ingest
python -m job_market extract itviec
python -m job_market extract topcv
cd analytics_dbt
dbt --profiles-dir . build
cd ..
python -m job_market sync-search
streamlit run app.py
```

The default dbt profile resolves its DuckDB path from `analytics_dbt/`. For a custom database, export the **same absolute** `DUCKDB_PATH` to the Python commands and to dbt. The CLI loads `.env`; dbt does not load that file by itself.

## Airflow

```sh
docker compose up -d --build
```

Open Airflow at `http://localhost:8080`, then unpause or trigger `it_job_market_etl_pipeline`. It is scheduled daily at 07:00 Vietnam time. The DAG uses the same CLI as the manual commands. Check the [operations guide](docs/operations.md) before running vector sync or maintenance.

The Compose setup is for local development. The embedded Qdrant directory inside the worker and the embedded directory used by host Streamlit are separate by default. Run `sync-search` on the host to update the host app's index; stop Streamlit first if it has the local index open. A shared Qdrant service and a durable sync checkpoint are separate deployment changes.

## Inspect and test

```sh
python -m job_market inspect warehouse
python -m job_market inspect search
python -m unittest discover -s tests -v
docker compose config --quiet
```

See [operations](docs/operations.md) for task-to-module mapping, failure diagnosis, and migration from the old `scripts/` commands.
