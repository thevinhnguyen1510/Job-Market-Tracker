{% macro normalize_location(location_column) %}
    CASE 
        WHEN LOWER({{ location_column }}) LIKE '%hồ chí minh%' 
          OR LOWER({{ location_column }}) LIKE '%ho chi minh%' 
          OR LOWER({{ location_column }}) LIKE '%hcm%' 
          OR LOWER({{ location_column }}) LIKE '%sài gòn%' THEN 'Ho Chi Minh'
        WHEN LOWER({{ location_column }}) LIKE '%hà nội%' 
          OR LOWER({{ location_column }}) LIKE '%ha noi%' 
          OR LOWER({{ location_column }}) LIKE '%hanoi%' THEN 'Ha Noi'
        WHEN LOWER({{ location_column }}) LIKE '%đà nẵng%' 
          OR LOWER({{ location_column }}) LIKE '%da nang%' 
          OR LOWER({{ location_column }}) LIKE '%danang%' THEN 'Da Nang'
        WHEN LOWER({{ location_column }}) LIKE '%remote%' 
          OR LOWER({{ location_column }}) LIKE '%từ xa%' THEN 'Remote'
        WHEN LOWER({{ location_column }}) LIKE '%hybrid%' THEN 'Hybrid'
        WHEN {{ location_column }} IS NULL OR TRIM({{ location_column }}) = '' THEN 'Unknown'
        ELSE TRIM({{ location_column }})
    END
{% endmacro %}
