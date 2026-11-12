# Warehouse transformations

- `models/sources.yml` declares the Bronze tables populated by the Python pipeline.
- `models/silver/` normalizes jobs, applies lifecycle rules and expands the job–skill relationship.
- `models/gold/` aggregates job demand, skills and salary distributions.
- `macros/` contains shared location and salary normalization.
- `seeds/tech_skill_taxonomy.csv` maps skills to categories.

From this directory, run `dbt --profiles-dir . build` after ingest and AI extraction. Airflow runs seed, Silver, Gold and tests as separate tasks for easier diagnosis. When overriding the DuckDB location, export the same absolute `DUCKDB_PATH` used by Python.
