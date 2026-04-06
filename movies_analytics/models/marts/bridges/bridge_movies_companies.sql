{{ config(materialized='table') }}

SELECT
    movie_id,
    UPPER(TRIM(REGEXP_REPLACE(company, '[\[\]\"㉿]', '', 'g'))) AS company_name
FROM {{ ref('stg_movies') }},
UNNEST(string_to_array(production_companies, ',')) AS company
WHERE production_companies IS NOT NULL
    AND UPPER(TRIM(REGEXP_REPLACE(company, '[\[\]\"㉿]', '', 'g'))) != ''