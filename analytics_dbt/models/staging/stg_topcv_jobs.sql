-- analytics_dbt/models/staging/stg_topcv_jobs.sql

WITH deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY job_id 
            ORDER BY crawl_timestamp DESC
        ) as rn
    FROM {{ source('raw', 'topcv_jobs') }}
)

SELECT 
    * EXCLUDE (rn)
FROM deduplicated
WHERE rn = 1