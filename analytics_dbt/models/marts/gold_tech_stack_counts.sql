{{ config(materialized='table') }}

WITH unnested_skills AS (
    SELECT 
        job_id,
        source,
        UNNEST(from_json(ai_core_tech_stack, '["VARCHAR"]')) AS skill
    FROM silver_all_jobs
    WHERE ai_core_tech_stack IS NOT NULL
      AND ai_core_tech_stack != '[]'
)

SELECT 
    skill,
    source,
    COUNT(DISTINCT job_id) AS total_mentions
FROM unnested_skills
WHERE skill != '' AND skill IS NOT NULL
GROUP BY 1, 2
ORDER BY total_mentions DESC