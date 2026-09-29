{{ config(
    materialized='view',
    description='View tương thích ngược (Backward compatibility) trỏ thẳng tới silver_jobs'
) }}

SELECT * FROM {{ ref('silver_jobs') }}
