{{ config(materialized='table') }}

SELECT
    m.movie_id,
    l->>'iso_639_1' AS language_code
FROM {{ ref('stg_movies') }} m,
LATERAL jsonb_array_elements(m.spoken_languages::jsonb) l
WHERE m.spoken_languages IS NOT NULL