{{ config(materialized='table') }}

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY (c->>'name')) AS country_id,
    c->>'name' AS country_name
FROM {{ ref('stg_movies') }},
LATERAL jsonb_array_elements(production_countries::jsonb) c
WHERE production_countries IS NOT NULL