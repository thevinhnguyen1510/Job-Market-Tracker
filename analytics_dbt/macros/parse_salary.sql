{% macro parse_salary_currency(salary_col) %}
    CASE 
        WHEN LOWER({{ salary_col }}) LIKE '%usd%' OR LOWER({{ salary_col }}) LIKE '%$%' THEN 'USD'
        WHEN LOWER({{ salary_col }}) LIKE '%triệu%' OR LOWER({{ salary_col }}) LIKE '%không lương%' THEN 'VND'
        ELSE 'OTHER'
    END
{% endmacro %}

{% macro parse_salary_min(salary_col) %}
    CASE
        WHEN LOWER({{ salary_col }}) LIKE '%không lương%' THEN 0.0
        WHEN LOWER({{ salary_col }}) LIKE '%usd%' AND regexp_matches({{ salary_col }}, '([0-9,]+)\s*-\s*([0-9,]+)')
            THEN ROUND(CAST(REPLACE(regexp_extract({{ salary_col }}, '([0-9,]+)\s*-\s*([0-9,]+)', 1), ',', '') AS DOUBLE) * 25.4 / 1000.0, 1)
        WHEN LOWER({{ salary_col }}) LIKE '%usd%' AND regexp_matches({{ salary_col }}, '(?i)(từ|trên)\s*([0-9,]+)')
            THEN ROUND(CAST(REPLACE(regexp_extract({{ salary_col }}, '(?i)(từ|trên)\s*([0-9,]+)', 2), ',', '') AS DOUBLE) * 25.4 / 1000.0, 1)
        WHEN LOWER({{ salary_col }}) LIKE '%triệu%' AND regexp_matches({{ salary_col }}, '([0-9.]+)\s*-\s*([0-9.]+)')
            THEN CAST(regexp_extract({{ salary_col }}, '([0-9.]+)\s*-\s*([0-9.]+)', 1) AS DOUBLE)
        WHEN LOWER({{ salary_col }}) LIKE '%triệu%' AND regexp_matches({{ salary_col }}, '(?i)(từ|trên)\s*([0-9.]+)')
            THEN CAST(regexp_extract({{ salary_col }}, '(?i)(từ|trên)\s*([0-9.]+)', 2) AS DOUBLE)
        ELSE NULL
    END
{% endmacro %}

{% macro parse_salary_max(salary_col) %}
    CASE
        WHEN LOWER({{ salary_col }}) LIKE '%không lương%' THEN 0.0
        WHEN LOWER({{ salary_col }}) LIKE '%usd%' AND regexp_matches({{ salary_col }}, '([0-9,]+)\s*-\s*([0-9,]+)')
            THEN ROUND(CAST(REPLACE(regexp_extract({{ salary_col }}, '([0-9,]+)\s*-\s*([0-9,]+)', 2), ',', '') AS DOUBLE) * 25.4 / 1000.0, 1)
        WHEN LOWER({{ salary_col }}) LIKE '%usd%' AND regexp_matches({{ salary_col }}, '(?i)(tới|đến|up to)\s*([0-9,]+)')
            THEN ROUND(CAST(REPLACE(regexp_extract({{ salary_col }}, '(?i)(tới|đến|up to)\s*([0-9,]+)', 2), ',', '') AS DOUBLE) * 25.4 / 1000.0, 1)
        WHEN LOWER({{ salary_col }}) LIKE '%triệu%' AND regexp_matches({{ salary_col }}, '([0-9.]+)\s*-\s*([0-9.]+)')
            THEN CAST(regexp_extract({{ salary_col }}, '([0-9.]+)\s*-\s*([0-9.]+)', 2) AS DOUBLE)
        WHEN LOWER({{ salary_col }}) LIKE '%triệu%' AND regexp_matches({{ salary_col }}, '(?i)(tới|đến|up to)\s*([0-9.]+)')
            THEN CAST(regexp_extract({{ salary_col }}, '(?i)(tới|đến|up to)\s*([0-9.]+)', 2) AS DOUBLE)
        ELSE NULL
    END
{% endmacro %}

{% macro is_salary_negotiable(salary_col) %}
    CASE
        WHEN {{ salary_col }} IS NULL OR TRIM({{ salary_col }}) = '' THEN TRUE
        WHEN LOWER({{ salary_col }}) IN ('thoả thuận', 'thỏa thuận', 'thương lượng', 'negotiable', 'sign in to view salary') 
          OR LOWER({{ salary_col }}) LIKE '%sign in%' THEN TRUE
        ELSE FALSE
    END
{% endmacro %}
