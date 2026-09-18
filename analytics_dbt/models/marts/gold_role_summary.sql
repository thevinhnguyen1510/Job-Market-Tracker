{{ config(materialized='table') }}

WITH base_data AS (
    SELECT 
        ai_job_role AS job_role,
        source,
        min_years_of_experience
    FROM {{ source('silver', 'all_jobs') }} 
    WHERE ai_job_role != 'Unknown' 
      AND ai_job_role != 'Error'
      AND status = 'Active'
)

SELECT 
    job_role,
    source,
    COUNT(*) AS total_jobs,
    ROUND(AVG(min_years_of_experience), 1) AS avg_years_required
FROM base_data
GROUP BY 1, 2
ORDER BY total_jobs DESC