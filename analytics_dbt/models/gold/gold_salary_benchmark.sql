{{ config(
    materialized='table',
    description='Thống kê phân vị mức lương (triệu VND) chi tiết theo từng Vị trí (Role) và Cấp bậc (Level)'
) }}

WITH salary_data AS (
    SELECT 
        ai_job_role AS job_role,
        job_level,
        source,
        salary_avg_vnd,
        salary_min_vnd,
        salary_max_vnd
    FROM {{ ref('silver_jobs') }}
    WHERE salary_avg_vnd IS NOT NULL
      AND ai_job_role NOT IN ('Unknown', 'Error')
      AND job_level NOT IN ('Unknown', 'Error')
      AND status = 'Active'
)

SELECT 
    job_role,
    job_level,
    COUNT(*) AS total_salary_disclosed_jobs,
    ROUND(MIN(salary_min_vnd), 1) AS min_salary_vnd,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_avg_vnd), 1) AS p25_salary_vnd,
    ROUND(MEDIAN(salary_avg_vnd), 1) AS median_salary_vnd,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_avg_vnd), 1) AS p75_salary_vnd,
    ROUND(MAX(salary_max_vnd), 1) AS max_salary_vnd,
    ROUND(AVG(salary_avg_vnd), 1) AS avg_salary_vnd
FROM salary_data
GROUP BY 1, 2
ORDER BY total_salary_disclosed_jobs DESC, median_salary_vnd DESC
