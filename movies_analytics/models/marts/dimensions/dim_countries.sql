{{ config(materialized='table') }}

SELECT DISTINCT
    c->> 'iso_3166_1' AS country_code,
    c->>'name' AS country_name
FROM {{ ref('stg_movies') }},
LATERAL jsonb_array_elements(production_countries::jsonb) c
WHERE production_countries IS NOT NULL
ORDER BY country_code