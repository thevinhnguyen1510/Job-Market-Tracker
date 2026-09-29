{{ config(
    materialized='table',
    description='Bảng dữ liệu việc làm tổng hợp tầng Silver đã được chuẩn hóa mức lương, theo dõi vòng đời first_seen_at và days_open, kết hợp trích xuất từ AI'
) }}

WITH unioned_raw AS (
    SELECT 
        job_id, job_url, job_title, company_name, location,
        salary_raw, tech_stack, source, crawl_timestamp,
        experience_level, job_category, job_description,
        first_seen_at,
        ROW_NUMBER() OVER (
            PARTITION BY job_id 
            ORDER BY crawl_timestamp DESC
        ) as rn
    FROM (
        SELECT * FROM {{ source('bronze', 'itviec_jobs') }}
        UNION ALL
        SELECT * FROM {{ source('bronze', 'topcv_jobs') }}
    )
),

dedup_jobs AS (
    SELECT * EXCLUDE (rn)
    FROM unioned_raw
    WHERE rn = 1
),

ai_data AS (
    SELECT * FROM {{ source('bronze', 'ai_extractions') }}
),

computed_silver AS (
    SELECT
        j.job_id,
        j.job_url,
        j.job_title,
        j.company_name,
        {{ normalize_location('j.location') }} AS location,
        j.location AS location_raw,
        j.salary_raw,
        {{ parse_salary_currency('j.salary_raw') }} AS salary_currency,
        {{ parse_salary_min('j.salary_raw') }} AS salary_min_vnd,
        {{ parse_salary_max('j.salary_raw') }} AS salary_max_vnd,
        {{ is_salary_negotiable('j.salary_raw') }} AS is_salary_negotiable,
        j.source,
        j.crawl_timestamp,
        j.experience_level,
        j.job_category,
        j.job_description,
        COALESCE(a.min_years_of_experience, 0) AS min_years_of_experience,
        COALESCE(a.ai_core_tech_stack, '[]') AS ai_core_tech_stack,
        COALESCE(a.english_requirement, 'None') AS english_requirement,
        CASE 
            WHEN a.ai_job_role IS NOT NULL AND a.ai_job_role != 'Unknown' THEN a.ai_job_role
            WHEN LOWER(j.job_title) LIKE '%backend%' THEN 'Backend'
            WHEN LOWER(j.job_title) LIKE '%software engineer%' 
              OR LOWER(j.job_title) LIKE '%software developer%' 
              OR LOWER(j.job_title) LIKE '%kỹ sư phần mềm%' 
              OR LOWER(j.job_title) LIKE '%embedded%sw%'
              OR LOWER(j.job_title) LIKE '%embedded software%' THEN 'Backend'
            WHEN LOWER(j.job_title) LIKE '%data engineer%' OR LOWER(j.job_title) LIKE '%kỹ sư dữ liệu%' THEN 'Data Engineer'
            WHEN LOWER(j.job_title) LIKE '%data analyst%' OR LOWER(j.job_title) LIKE '%phân tích dữ liệu%' OR LOWER(j.job_title) LIKE '%bi analyst%' THEN 'Data/Business Analyst'
            WHEN LOWER(j.job_title) LIKE '%ai engineer%' OR LOWER(j.job_title) LIKE '%machine learning%' OR LOWER(j.job_title) LIKE '%nlp engineer%' THEN 'AI/Machine Learning'
            ELSE COALESCE(a.ai_job_role, 'Unknown')
        END AS ai_job_role,
        COALESCE(a.job_level, 'Unknown') AS job_level,
        a.processed_at,
        LEAST(j.crawl_timestamp, COALESCE(j.first_seen_at, a.processed_at, j.crawl_timestamp)) AS first_seen_at,
        j.crawl_timestamp AS last_seen_at,
        CASE 
            WHEN j.job_description = 'EXPIRED' THEN 'Inactive'
            WHEN j.crawl_timestamp < (CURRENT_TIMESTAMP - INTERVAL '7 days') THEN 'Inactive'
            ELSE 'Active'
        END AS status
    FROM dedup_jobs j
    LEFT JOIN ai_data a ON j.job_id = a.job_id
)

SELECT
    *,
    CASE
        WHEN salary_min_vnd IS NOT NULL AND salary_max_vnd IS NOT NULL
            THEN ROUND((salary_min_vnd + salary_max_vnd) / 2.0, 1)
        ELSE COALESCE(salary_min_vnd, salary_max_vnd)
    END AS salary_avg_vnd,
    CASE
        WHEN status = 'Active' 
            THEN GREATEST(0, DATE_DIFF('day', CAST(first_seen_at AS DATE), CURRENT_DATE))
        ELSE GREATEST(0, DATE_DIFF('day', CAST(first_seen_at AS DATE), CAST(last_seen_at AS DATE)))
    END AS days_open
FROM computed_silver
