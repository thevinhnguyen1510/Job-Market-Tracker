{{ config(materialized='table') }}
-- models/gold/gold_tech_stack_counts.sql

with silver_jobs as (
    select * from silver_itviec_jobs
),

-- 1. Clean string and split array into rows
unnested_skills as (
    select
        job_level,
        trim(unnest(string_split(
            replace(replace(replace(ai_core_tech_stack, '[', ''), ']', ''), '"', ''), 
            ','
        ))) as skill
    from silver_jobs
    where ai_core_tech_stack is not null
)

-- 2. Calculate mention counts by Level
select
    job_level,
    skill,
    count(*) as mentions
from unnested_skills
where skill != ''
group by 1, 2
order by mentions desc