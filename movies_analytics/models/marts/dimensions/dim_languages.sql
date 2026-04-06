{{ config(materialized='table') }}

SELECT DISTINCT
    l->>'iso_639_1' AS language_code,
    l->>'name' AS language_name
FROM {{ ref('stg_movies') }} m,
LATERAL jsonb_array_elements(m.spoken_languages::jsonb) l
WHERE m.spoken_languages IS NOT NULL
ORDER BY language_code