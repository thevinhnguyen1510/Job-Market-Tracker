{{ config(
    materialized='table',
    description='Thống kê tổng số lượt yêu cầu của từng kỹ năng công nghệ kèm nhóm phân loại'
) }}

SELECT 
    skill_name,
    skill_category,
    source,
    COUNT(DISTINCT job_id) AS total_mentions
FROM {{ ref('silver_job_skills') }}
WHERE status = 'Active'
GROUP BY 1, 2, 3
ORDER BY total_mentions DESC
