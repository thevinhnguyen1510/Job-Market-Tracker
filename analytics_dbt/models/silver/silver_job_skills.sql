{{ config(
    materialized='table',
    description='Bảng cầu quan hệ N-N giữa jobs và skills đã được unnest và phân loại nhóm công nghệ'
) }}

WITH unnested_skills AS (
    SELECT 
        job_id,
        source,
        job_level,
        ai_job_role AS job_role,
        status,
        TRIM(UNNEST(from_json(ai_core_tech_stack, '["VARCHAR"]'))) AS skill_name
    FROM {{ ref('silver_jobs') }}
    WHERE ai_core_tech_stack IS NOT NULL
      AND ai_core_tech_stack != '[]'
      AND ai_core_tech_stack != ''
)

SELECT 
    u.job_id,
    u.source,
    u.job_level,
    u.job_role,
    u.status,
    u.skill_name,
    COALESCE(t.skill_category, 'Other / Specialized') AS skill_category
FROM unnested_skills u
LEFT JOIN {{ ref('tech_skill_taxonomy') }} t 
    ON LOWER(u.skill_name) = LOWER(t.skill_name)
WHERE u.skill_name != '' AND u.skill_name IS NOT NULL
