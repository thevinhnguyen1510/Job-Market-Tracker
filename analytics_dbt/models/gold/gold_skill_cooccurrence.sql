{{ config(
    materialized='table',
    description='Ma trận tương quan giữa các kỹ năng thường xuyên xuất hiện cùng nhau trong một bài đăng'
) }}

WITH active_skills AS (
    SELECT DISTINCT 
        job_id, 
        skill_name
    FROM {{ ref('silver_job_skills') }}
    WHERE status = 'Active'
)

SELECT 
    s1.skill_name AS skill_a,
    s2.skill_name AS skill_b,
    COUNT(*) AS cooccurrence_count
FROM active_skills s1
JOIN active_skills s2 
  ON s1.job_id = s2.job_id 
 AND s1.skill_name < s2.skill_name
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY cooccurrence_count DESC
