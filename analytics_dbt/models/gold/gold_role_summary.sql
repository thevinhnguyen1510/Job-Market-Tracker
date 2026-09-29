{{ config(
    materialized='table',
    description='Tổng hợp số lượng việc làm và số năm kinh nghiệm trung bình theo từng Role'
) }}

WITH base_data AS (
    SELECT 
        ai_job_role AS job_role,
        source,
        min_years_of_experience
    FROM {{ ref('silver_jobs') }} 
    WHERE ai_job_role NOT IN ('Unknown', 'Error')
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
