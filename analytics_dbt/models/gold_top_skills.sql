{{ config(materialized='view') }}

WITH cleaned_strings AS (
    SELECT 
        job_url,
        REPLACE(REPLACE(REPLACE(ai_core_tech_stack, '[', ''), ']', ''), '"', '') AS clean_stack
    FROM silver_itviec_jobs
    WHERE ai_core_tech_stack IS NOT NULL
),
unnested_skills AS (
    SELECT 
        job_url,
        TRIM(UNNEST(string_split(clean_stack, ','))) AS skill
    FROM cleaned_strings
    WHERE clean_stack != ''
)

SELECT 
    skill,
    COUNT(DISTINCT job_url) AS total_mentions
FROM unnested_skills
WHERE skill != '' 
GROUP BY skill
ORDER BY total_mentions DESC