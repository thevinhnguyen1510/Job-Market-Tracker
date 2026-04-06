{{ config(materialized='table') }}

WITH unnested_skills AS (
    SELECT
        job_level,
        source,
        UNNEST(from_json(ai_core_tech_stack, '["VARCHAR"]')) AS skill
    FROM silver_all_jobs
    WHERE ai_core_tech_stack IS NOT NULL
      AND ai_core_tech_stack != '[]'
      AND job_level != 'Error'
      AND job_level != 'Unknown'
)

SELECT
    job_level,
    skill,
    source,
    COUNT(*) AS mentions
FROM unnested_skills
WHERE skill != '' AND skill IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY mentions DESC