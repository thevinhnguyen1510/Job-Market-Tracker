-- analytics_dbt/models/intermediate/int_all_jobs.sql

{{ config(materialized='table') }}

WITH topcv AS (
    SELECT * FROM {{ ref('stg_topcv_jobs') }}
),

itviec AS (
    SELECT * FROM {{ ref('stg_itviec_jobs') }}
)

SELECT * FROM topcv
UNION ALL
SELECT * FROM itviec