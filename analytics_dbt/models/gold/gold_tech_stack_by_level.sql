{{ config(
    materialized='table',
    description='Thống kê kỹ năng công nghệ được săn đón theo từng cấp bậc (Intern, Junior, Middle, Senior...)'
) }}

SELECT 
    skill_name,
    skill_category,
    job_level,
    COUNT(DISTINCT job_id) AS total_mentions
FROM {{ ref('silver_job_skills') }}
WHERE status = 'Active'
  AND job_level NOT IN ('Unknown', 'Error')
GROUP BY 1, 2, 3
ORDER BY job_level, total_mentions DESC
