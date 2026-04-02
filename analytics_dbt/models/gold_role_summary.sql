{{ config(materialized='view') }}

SELECT 
    ai_job_role AS job_role,
    COUNT(*) AS total_jobs,
    ROUND(AVG(min_years_of_experience), 1) AS avg_years_required
FROM silver_itviec_jobs
WHERE ai_job_role != 'Unknown'
GROUP BY ai_job_role
ORDER BY total_jobs DESC